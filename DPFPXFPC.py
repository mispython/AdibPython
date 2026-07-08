#!/usr/bin/env python3
"""
EIBDAWSA - Average Savings Account Analysis
Memory-optimized processing of DPTRBLGS parquet data for savings account statistics
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Format mappings
SACUSTCD_MAP = {
    '001': '01', '002': '02', '003': '03', '004': '04',
}

SAPROD_MAP = {
    204: '20100', 207: '20200', 214: '20300', 215: '20400',
}

CAPROD_MAP = {
    93: '10100',
}

SADENOM_MAP = {
    204: 'D', 207: 'D', 214: 'I', 215: 'I',
}

CADENOM_MAP = {
    93: 'D',
}

DDRANGE_MAP = [
    (0, 1000, '< 1000'),
    (1000, 5000, '1000 - 4999'),
    (5000, 10000, '5000 - 9999'),
    (10000, 50000, '10000 - 49999'),
    (50000, float('inf'), '50000 & ABOVE'),
]

CARANGE_MAP = [
    (0, 1000, '< 1000'),
    (1000, 5000, '1000 - 4999'),
    (5000, 10000, '5000 - 9999'),
    (10000, 50000, '10000 - 49999'),
    (50000, 100000, '50000 - 99999'),
    (100000, float('inf'), '100000 & ABOVE'),
]

ISARANGE_MAP = [
    (0, 1000, '< 1000'),
    (1000, 5000, '1000 - 4999'),
    (5000, 10000, '5000 - 9999'),
    (10000, 50000, '10000 - 49999'),
    (50000, 100000, '50000 - 99999'),
    (100000, float('inf'), '100000 & ABOVE'),
]

IBWRNGE_MAP = [
    (0, 1000, '< 1000'),
    (1000, 5000, '1000 - 4999'),
    (5000, 10000, '5000 - 9999'),
    (10000, 50000, '10000 - 49999'),
    (50000, 100000, '50000 - 99999'),
    (100000, float('inf'), '100000 & ABOVE'),
]

def get_state_code(branch):
    """Get state code from branch number"""
    if branch is None or pd.isna(branch):
        return 'XX'
    try:
        branch = int(branch)
        if 100 <= branch <= 199:
            return 'JH'
        elif 200 <= branch <= 299:
            return 'KD'
        elif 300 <= branch <= 399:
            return 'KL'
        else:
            return 'XX'
    except:
        return 'XX'

def create_range_case_statement(range_map, column_name):
    """Create a SQL CASE statement for range bucketing"""
    cases = []
    for low, high, label in range_map:
        if high == float('inf'):
            cases.append(f"WHEN {column_name} >= {low} THEN '{label}'")
        else:
            cases.append(f"WHEN {column_name} >= {low} AND {column_name} < {high} THEN '{label}'")
    return f"CASE {' '.join(cases)} ELSE 'UNKNOWN' END"

def process_with_duckdb(input_file, reptdate, reptmon, reptday, reptyear):
    """Process data using DuckDB for memory efficiency"""
    
    print("Initializing DuckDB connection...")
    con = duckdb.connect(':memory:')
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA threads=4")
    
    # Register the parquet file as a view
    print("Registering parquet file...")
    con.execute(f"""
        CREATE OR REPLACE VIEW dptrblgs AS 
        SELECT * FROM parquet_scan('{input_file}')
    """)
    
    # Filter initial records to reduce data volume
    print("Filtering records...")
    con.execute("""
        CREATE OR REPLACE VIEW filtered_records AS
        SELECT 
            BANKNO, REPTNO, FMTCODE, BRANCH, ACCTNO,
            DEBIT, CREDIT, CLOSEDT, OPENDT, CUSTCODE,
            PURPOSE, OPENIND, AVGAMT, PRODUCT, RACE,
            DEPTYPE, INT1, CURBAL, INT2, APPRLIMT, BDATE
        FROM dptrblgs
        WHERE BANKNO = 33 
          AND REPTNO = 1001 
          AND FMTCODE = 1
          AND OPENIND NOT IN ('B', 'C', 'P')
          AND PRODUCT NOT IN (297, 298)
    """)
    
    # Get count after filtering
    count = con.execute("SELECT COUNT(*) FROM filtered_records").fetchone()[0]
    print(f"Filtered records: {count:,}")
    
    if count == 0:
        print("No records after filtering")
        con.close()
        return None
    
    print("Processing and aggregating data...")
    
    # Create range case statements
    dd_range_case = create_range_case_statement(DDRANGE_MAP, 'AVGAMT')
    ca_range_case = create_range_case_statement(CARANGE_MAP, 'CURBAL')
    isa_range_case = create_range_case_statement(ISARANGE_MAP, 'CURBAL')
    ibw_range_case = create_range_case_statement(IBWRNGE_MAP, 'CURBAL')
    
    # Process data using SQL with all logic in DuckDB
    query = f"""
    WITH processed AS (
        SELECT 
            PRODUCT,
            BRANCH,
            ACCTNO,
            CASE 
                WHEN PURPOSE IS NULL OR PURPOSE = '' THEN '0'
                ELSE CAST(PURPOSE AS VARCHAR)
            END AS PURPOSE,
            CASE 
                WHEN RACE IS NULL OR RACE = '' THEN '0'
                ELSE CAST(RACE AS VARCHAR)
            END AS RACE,
            CASE 
                WHEN CUSTCODE IS NULL THEN '99'
                WHEN CUSTCODE = 1 THEN '01'
                WHEN CUSTCODE = 2 THEN '02'
                WHEN CUSTCODE = 3 THEN '03'
                WHEN CUSTCODE = 4 THEN '04'
                ELSE '99'
            END AS CUSTCD,
            {dd_range_case} AS AVGRNGE,
            CURBAL,
            AVGAMT,
            CASE 
                WHEN OPENDT != 0 AND CLOSEDT = 0 
                     AND CAST(SUBSTR(CAST(OPENDT AS VARCHAR), 1, 2) AS INTEGER) = {reptyear - 2000}
                THEN 1
                ELSE 0
            END AS ACCYTD,
            CASE 
                WHEN PRODUCT IN (204, 207) THEN 'ALWSA'
                WHEN PRODUCT = 215 THEN 'ALWSS'
                ELSE 'OTHER'
            END AS TYPE,
            CASE 
                WHEN PRODUCT = 214 THEN {isa_range_case}
                WHEN PRODUCT = 207 THEN {ibw_range_case}
                ELSE {ca_range_case}
            END AS DEPRANGE,
            {reptday} AS REPTDAY,
            {reptmon} AS REPTMON,
            '{reptdate.strftime('%Y-%m-%d')}'::DATE AS REPTDATE
        FROM filtered_records
        WHERE PRODUCT IN (204, 207, 215)
    )
    SELECT 
        PURPOSE,
        RACE,
        CUSTCD,
        AVGRNGE,
        DEPRANGE,
        PRODUCT,
        COUNT(ACCTNO) AS NOACCT,
        SUM(CURBAL) AS CURBAL,
        COUNT(AVGAMT) AS AVGACCT,
        SUM(AVGAMT) AS AVGAMT,
        SUM(ACCYTD) AS ACCYTD,
        REPTDATE,
        CASE 
            WHEN PRODUCT = 204 THEN 'D'
            WHEN PRODUCT = 207 THEN 'D'
            WHEN PRODUCT = 214 THEN 'I'
            WHEN PRODUCT = 215 THEN 'I'
            ELSE 'D'
        END AS DENOM,
        CASE 
            WHEN PRODUCT = 204 THEN '20100'
            WHEN PRODUCT = 207 THEN '20200'
            WHEN PRODUCT = 214 THEN '20300'
            WHEN PRODUCT = 215 THEN '20400'
            ELSE '00000'
        END AS PRODCD
    FROM processed
    GROUP BY PURPOSE, RACE, CUSTCD, AVGRNGE, DEPRANGE, PRODUCT, REPTDATE
    ORDER BY PRODUCT, PURPOSE, RACE, CUSTCD
    """
    
    print("Executing aggregation query...")
    aggregated = con.execute(query).fetchdf()
    
    print(f"Aggregated records: {len(aggregated):,}")
    
    # Clean up
    con.close()
    gc.collect()
    
    return aggregated

def output_sas_dataset(df, dataset_name, output_dir):
    """Output dataframe as SAS dataset using saspy"""
    try:
        print(f"Creating SAS dataset: {dataset_name}")
        import saspy
        sas = saspy.SASsession()
        
        # Convert to SAS dataset
        sas_df = sas.dataframe2sasdata(df, table=dataset_name, libref='work')
        
        # Export to SAS
        sas.saslib('outlib', path=str(output_dir), engine='base')
        sas.submit(f"""
            data outlib.{dataset_name};
                set work.{dataset_name};
            run;
        """)
        
        print(f"SAS dataset created: {output_dir}/{dataset_name}.sas7bdat")
        sas.endsas()
        return True
    except Exception as e:
        print(f"Warning: Error creating SAS dataset: {e}")
        print("Skipping SAS output...")
        return False

def output_parquet(df, dataset_name, output_dir):
    """Output dataframe as parquet file"""
    try:
        parquet_path = output_dir / f"{dataset_name}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
        print(f"Parquet file created: {parquet_path}")
        return True
    except Exception as e:
        print(f"Error creating parquet file: {e}")
        return False

def output_csv(df, dataset_name, output_dir):
    """Output dataframe as CSV file (backup format)"""
    try:
        csv_path = output_dir / f"{dataset_name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"CSV file created: {csv_path}")
        return True
    except Exception as e:
        print(f"Error creating CSV file: {e}")
        return False

print("EIBDAWSA - Average Savings Account Analysis")
print("="*80)

# Get current date and time
reptdate = datetime.now()
day = reptdate.day

# Determine week
if 1 <= day <= 8:
    nowk = '1'
elif 9 <= day <= 15:
    nowk = '2'
elif 16 <= day <= 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.year
reptmon = reptdate.month
reptday = day
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Report Date: {rdate}")
print(f"Week: {nowk}")
print(f"Year: {reptyear}, Month: {reptmon:02d}, Day: {reptday:02d}")

# Process DPTRBLGS parquet file
input_file = INPUT_DIR / 'DPTRBLGS.parquet'
if not input_file.exists():
    print(f"Error: Input file {input_file} not found!")
    exit(1)

print(f"Input file: {input_file}")
print(f"File size: {input_file.stat().st_size / (1024**3):.2f} GB")

try:
    # Process data using DuckDB
    aggregated_df = process_with_duckdb(input_file, reptdate, reptmon, reptday, reptyear)
    
    if aggregated_df is not None and not aggregated_df.empty:
        # Output monthly dataset
        dataset_name = f"awsa{reptmon:02d}"
        
        # Output in multiple formats
        output_parquet(aggregated_df, dataset_name, OUTPUT_DIR)
        output_csv(aggregated_df, dataset_name, OUTPUT_DIR)
        
        # Try SAS output
        try:
            output_sas_dataset(aggregated_df, dataset_name, OUTPUT_DIR)
        except Exception as e:
            print(f"SAS output skipped: {e}")
        
        print("\n" + "="*80)
        print("AGGREGATION SUMMARY")
        print("="*80)
        print(f"Total accounts: {aggregated_df['NOACCT'].sum():,}")
        print(f"Total balance: {aggregated_df['CURBAL'].sum():,.2f}")
        print(f"Total average amount: {aggregated_df['AVGAMT'].sum():,.2f}")
        print(f"New accounts this year: {aggregated_df['ACCYTD'].sum():,}")
        print("\nProducts processed:")
        product_summary = aggregated_df.groupby('PRODUCT')['NOACCT'].sum()
        for prod, count in product_summary.items():
            print(f"  Product {prod}: {count:,} accounts")
        
        # Show sample of output
        print("\nSample output (first 5 rows):")
        print(aggregated_df.head(5).to_string())
        
        print("\n" + "="*80)
        print("OUTPUT STRUCTURE")
        print("="*80)
        print(f"""
Dataset: {dataset_name} (Parquet, CSV, and SAS formats)
Location: {OUTPUT_DIR}
Fields: PURPOSE, RACE, CUSTCD, AVGRNGE, DEPRANGE, PRODUCT,
        NOACCT, CURBAL, AVGACCT, AVGAMT, ACCYTD, REPTDATE,
        DENOM, PRODCD

Products: 204, 207 (Savings), 215 (Special), 93 (Wadiah)
Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
        """)
    else:
        print("No data to output")

except MemoryError as e:
    print(f"ERROR: Out of memory - {e}")
    print("Try running with more memory or split the input file")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

print(f"\nCompleted. Output files in: {OUTPUT_DIR}")
