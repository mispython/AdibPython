#!/usr/bin/env python3
"""
EIBDAWSA - Average Savings Account Analysis
Processes DPTRBLGS parquet data for savings account statistics
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import saspy
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Format mappings from PBMISFMT and PBBDPFMT
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

DDRANGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, float('inf')): '50000 & ABOVE',
}

CARANGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, float('inf')): '100000 & ABOVE',
}

ISARANGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, float('inf')): '100000 & ABOVE',
}

IBWRNGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, float('inf')): '100000 & ABOVE',
}

STATE_MAP = {
    range(100, 200): 'JH',
    range(200, 300): 'KD',
    range(300, 400): 'KL',
}

def parse_packed_decimal(data, precision, scale=0):
    """Parse packed decimal from bytes"""
    if not data or len(data) == 0:
        return 0
    try:
        hex_str = data.hex()
        if not hex_str:
            return 0
        sign = 1 if hex_str[-1] in ['C', 'F'] else -1
        digits = hex_str[:-1]
        if not digits:
            return 0
        value = int(digits) * sign
        return value / (10 ** scale)
    except:
        return 0

def get_range_bucket(value, range_map):
    """Get bucket label for a value based on range map"""
    for (low, high), label in range_map.items():
        if low <= value < high:
            return label
    return 'UNKNOWN'

def get_state_code(branch):
    """Get state code from branch number"""
    branch_prefix = (branch // 100) * 100
    for r, state in STATE_MAP.items():
        if branch_prefix in r:
            return state
    return 'XX'

def calculate_age(bdate, reptdate, reptmon, reptday, reptyear):
    """Calculate age category based on birth date and report date"""
    if pd.isna(bdate) or bdate == 0:
        return 0
    
    try:
        bdate_str = str(int(bdate)).zfill(6)
        bday = int(bdate_str[4:6])
        bmonth = int(bdate_str[2:4])
        byear = int(bdate_str[0:2])
        
        # Handle year properly (assuming 1900s)
        if byear > 50:
            byear = 1900 + byear
        else:
            byear = 2000 + byear
        
        age = reptyear - byear
        
        if age == AGELIMIT:
            if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                age = AGEBELOW
        elif age == MAXAGE:
            if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
                age = AGELIMIT
        elif age > MAXAGE:
            age = MAXAGE
        elif age < AGELIMIT:
            age = AGEBELOW
        else:
            age = AGELIMIT
        
        return age
    except:
        return 0

def process_dptrblgs_parquet(input_file):
    """Process DPTRBLGS parquet file and extract savings account data"""
    print(f"Reading parquet file: {input_file}")
    
    # Read parquet file
    df = pd.read_parquet(input_file)
    print(f"Total records read: {len(df)}")
    
    # Convert byte fields to Python objects for processing
    savings_data = []
    
    for idx, row in df.iterrows():
        try:
            # Parse fields from parquet
            bankno = row.get('BANKNO', 0)
            reptno = row.get('REPTNO', 0)
            fmtcode = row.get('FMTCODE', 0)
            
            # Filter: bankno=33, reptno=1001, fmtcode=1
            if bankno != 33 or reptno != 1001 or fmtcode != 1:
                continue
            
            # Parse packed decimal fields
            branch = row.get('BRANCH', 0)
            acctno = row.get('ACCTNO', 0)
            
            # Handle string fields (EBCDIC encoded)
            name_bytes = row.get('NAME', b'')
            try:
                name = name_bytes.decode('cp037') if isinstance(name_bytes, bytes) else str(name_bytes)
            except:
                name = ''
            
            # Parse numeric fields (already decoded in parquet)
            debit = row.get('DEBIT', 0.0)
            credit = row.get('CREDIT', 0.0)
            closedt = row.get('CLOSEDT', 0)
            opendt = row.get('OPENDT', 0)
            custcode = row.get('CUSTCODE', 0)
            
            purpose_bytes = row.get('PURPOSE', b'')
            try:
                purpose = purpose_bytes.decode('cp037') if isinstance(purpose_bytes, bytes) else str(purpose_bytes)
            except:
                purpose = '0'
            
            openind_bytes = row.get('OPENIND', b'')
            try:
                openind = openind_bytes.decode('cp037') if isinstance(openind_bytes, bytes) else str(openind_bytes)
            except:
                openind = ''
            
            avgamt = row.get('AVGAMT', 0)
            product = row.get('PRODUCT', 0)
            
            race_bytes = row.get('RACE', b'')
            try:
                race = race_bytes.decode('cp037') if isinstance(race_bytes, bytes) else str(race_bytes)
            except:
                race = '0'
            
            deptype_bytes = row.get('DEPTYPE', b'')
            try:
                deptype = deptype_bytes.decode('cp037') if isinstance(deptype_bytes, bytes) else str(deptype_bytes)
            except:
                deptype = ''
            
            int1 = row.get('INT1', 0.0)
            curbal = row.get('CURBAL', 0.0)
            int2 = row.get('INT2', 0.0)
            apprlimt = row.get('APPRLIMT', 0)
            bdate = row.get('BDATE', 0)
            
            # Filter conditions
            if openind in ['B', 'C', 'P'] or product in [297, 298]:
                continue
            
            # Calculate interest payable
            intpaybl = int1 + int2
            
            # Check if account opened this year
            accytd = 0
            if opendt != 0 and closedt == 0:
                open_year = int(str(int(opendt)).zfill(6)[:2])
                current_year = int(str(reptyear)[-2:])
                if open_year == current_year:
                    accytd = 1
            
            # Get mappings
            custcd = SACUSTCD_MAP.get(f"{int(custcode):03d}", '99')
            statecd = get_state_code(branch)
            prodcd = SAPROD_MAP.get(product, '00000')
            amtind = SADENOM_MAP.get(product, 'D')
            avgrnge = get_range_bucket(avgamt, DDRANGE_MAP)
            
            # Get range bucket based on product
            if product == 214:
                range_bucket = get_range_bucket(curbal, ISARANGE_MAP)
            elif product == 207:
                range_bucket = get_range_bucket(curbal, IBWRNGE_MAP)
            else:
                range_bucket = get_range_bucket(curbal, CARANGE_MAP)
            
            # Calculate age
            age = calculate_age(bdate, datetime.now(), reptmon, reptday, reptyear)
            
            # Add to savings data for eligible products
            if product in [204, 207]:
                savings_data.append({
                    'PRODUCT': product,
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'PURPOSE': purpose if purpose and purpose != '' else '0',
                    'RACE': race if race and race != '' else '0',
                    'CUSTCD': custcd,
                    'AVGRNGE': avgrnge,
                    'CURBAL': curbal,
                    'AVGAMT': avgamt,
                    'ACCYTD': accytd,
                    'TYPE': 'ALWSA',
                    'DEPRANGE': range_bucket,
                    'REPTDATE': datetime.now(),
                    'REPTMON': reptmon,
                    'REPTDAY': reptday
                })
            elif product == 215:
                savings_data.append({
                    'PRODUCT': product,
                    'BRANCH': branch,
                    'ACCTNO': acctno,
                    'PURPOSE': purpose if purpose and purpose != '' else '0',
                    'RACE': race if race and race != '' else '0',
                    'CUSTCD': custcd,
                    'AVGRNGE': avgrnge,
                    'CURBAL': curbal,
                    'AVGAMT': avgamt,
                    'ACCYTD': accytd,
                    'TYPE': 'ALWSS',
                    'DEPRANGE': range_bucket,
                    'REPTDATE': datetime.now(),
                    'REPTMON': reptmon,
                    'REPTDAY': reptday
                })
                
        except Exception as e:
            print(f"Error processing record {idx}: {e}")
            continue
    
    print(f"Extracted {len(savings_data)} savings records")
    return pd.DataFrame(savings_data)

def aggregate_savings_data(df):
    """Aggregate savings data by dimensions"""
    if df.empty:
        print("No data to aggregate")
        return pd.DataFrame()
    
    # Perform aggregation
    aggregated = df.groupby(['PURPOSE', 'RACE', 'CUSTCD', 'AVGRNGE', 'DEPRANGE', 'PRODUCT']).agg({
        'ACCTNO': 'count',
        'CURBAL': 'sum',
        'AVGAMT': ['count', 'sum'],
        'ACCYTD': 'sum'
    }).reset_index()
    
    # Flatten column names
    aggregated.columns = ['PURPOSE', 'RACE', 'CUSTCD', 'AVGRNGE', 'DEPRANGE', 'PRODUCT',
                          'NOACCT', 'CURBAL', 'AVGACCT', 'AVGAMT', 'ACCYTD']
    
    # Add report date
    aggregated['REPTDATE'] = datetime.now()
    
    return aggregated

def output_sas_dataset(df, dataset_name, output_dir):
    """Output dataframe as SAS dataset using saspy"""
    try:
        # Connect to SAS
        sas = saspy.SASsession()
        
        # Create SAS dataset
        sas.saslib('work', engine='base')
        
        # Convert pandas dataframe to SAS
        sas_df = sas.dataframe2sasdata(df, table=dataset_name, libref='work')
        
        # Export to SAS dataset
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
        print(f"Error creating SAS dataset: {e}")
        return False

def output_parquet(df, dataset_name, output_dir):
    """Output dataframe as parquet file"""
    try:
        parquet_path = output_dir / f"{dataset_name}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"Parquet file created: {parquet_path}")
        return True
    except Exception as e:
        print(f"Error creating parquet file: {e}")
        return False

def monthly_append(df, reptday, reptmon, output_dir):
    """Append or create monthly dataset"""
    dataset_name = f"awsa{reptmon:02d}"
    
    # Output both SAS and Parquet
    output_sas_dataset(df, dataset_name, output_dir)
    output_parquet(df, dataset_name, output_dir)
    
    return dataset_name

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
zdate = reptdate.strftime('%y%m%d')

print(f"Report Date: {rdate}")
print(f"Week: {nowk}")
print(f"Year: {reptyear}, Month: {reptmon:02d}, Day: {reptday:02d}")

# Process DPTRBLGS parquet file
input_file = INPUT_DIR / 'DPTRBLGS.parquet'
if not input_file.exists():
    print(f"Error: Input file {input_file} not found!")
    exit(1)

# Extract savings data
savings_df = process_dptrblgs_parquet(input_file)

if not savings_df.empty:
    # Aggregate data
    aggregated_df = aggregate_savings_data(savings_df)
    
    # Add denominator and product code mappings
    aggregated_df['DENOM'] = aggregated_df['PRODUCT'].map(SADENOM_MAP).fillna('D')
    aggregated_df['PRODCD'] = aggregated_df['PRODUCT'].map(SAPROD_MAP).fillna('00000')
    
    # Output monthly dataset
    dataset_name = monthly_append(aggregated_df, reptday, reptmon, OUTPUT_DIR)
    
    print(f"\nAggregated {len(aggregated_df)} records to {dataset_name}")
    print(f"Total savings accounts processed: {len(savings_df)}")
    
    # Print summary
    print("\n" + "="*80)
    print("AGGREGATION SUMMARY")
    print("="*80)
    print(f"Total accounts: {aggregated_df['NOACCT'].sum():,}")
    print(f"Total balance: {aggregated_df['CURBAL'].sum():,.2f}")
    print(f"Total average amount: {aggregated_df['AVGAMT'].sum():,.2f}")
    print(f"New accounts this year: {aggregated_df['ACCYTD'].sum():,}")
    print("\nProducts processed:")
    print(aggregated_df.groupby('PRODUCT')['NOACCT'].sum().to_string())
else:
    print("No savings data found to process")

print("\n" + "="*80)
print("OUTPUT STRUCTURE")
print("="*80)
print("""
Dataset: awsa{MM} (SAS and Parquet formats)
Fields: PURPOSE, RACE, CUSTCD, AVGRNGE, DEPRANGE, PRODUCT,
        NOACCT, CURBAL, AVGACCT, AVGAMT, ACCYTD, REPTDATE,
        DENOM, PRODCD

Products: 204, 207 (Savings), 215 (Special), 93 (Wadiah)
Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
""")

print(f"\nCompleted. Output files in: {OUTPUT_DIR}")
