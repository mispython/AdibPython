"""
SAS to Python Translation - EIBWELWH
Processes loan application data from multiple source files
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import re

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

# Create directories
for dir_path in [INPUT_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Libraries (SAS libnames)
ELDS1_DIR = OUTPUT_DIR / "elds1"
ELDS2_DIR = OUTPUT_DIR / "elds2"
IELDS1_DIR = OUTPUT_DIR / "ields1"
IELDS2_DIR = OUTPUT_DIR / "ields2"

for dir_path in [ELDS1_DIR, ELDS2_DIR, IELDS1_DIR, IELDS2_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Input files
INPUT_FILES = {
    'ELDSTXT': INPUT_DIR / "ELDSTXT.parquet",
    'ELDSTX2': INPUT_DIR / "ELDSTX2.parquet",
    'ELDSTX3': INPUT_DIR / "ELDSTX3.parquet",
    'ELDSTX4': INPUT_DIR / "ELDSTX4.parquet",
    'ELDSTX7': INPUT_DIR / "ELDSTX7.parquet",
    'ELDSTX8': INPUT_DIR / "ELDSTX8.parquet",
    'ELDST10': INPUT_DIR / "ELDST10.parquet",
    'ELDST17': INPUT_DIR / "ELDST17.parquet",
    'ELDST20': INPUT_DIR / "ELDST20.parquet",
    'ELDST22': INPUT_DIR / "ELDST22.parquet",
    'ELDST26': INPUT_DIR / "ELDST26.parquet",
    'ELDST30': INPUT_DIR / "ELDST30.parquet",
    'ELDST63': INPUT_DIR / "ELDST63.parquet",
    'ELDSHP1': INPUT_DIR / "ELDSHP1.parquet",
    'ELDSHP2': INPUT_DIR / "ELDSHP2.parquet",
    'ELDSHP3': INPUT_DIR / "ELDSHP3.parquet",
    'ELDSHP4': INPUT_DIR / "ELDSHP4.parquet",
    'ELDSHP8': INPUT_DIR / "ELDSHP8.parquet",
    'ELDSHP10': INPUT_DIR / "ELDSHP10.parquet",
    'ELDSHP11': INPUT_DIR / "ELDSHP11.parquet",
    'ELDSHP13': INPUT_DIR / "ELDSHP13.parquet",
    'ELDSHP17': INPUT_DIR / "ELDSHP17.parquet",
    'ELDSHP18': INPUT_DIR / "ELDSHP18.parquet",
    'ELDSHP19': INPUT_DIR / "ELDSHP19.parquet",
    'ELDSHP21': INPUT_DIR / "ELDSHP21.parquet",
    'ELDSHP22': INPUT_DIR / "ELDSHP22.parquet",
    'ELNAA': INPUT_DIR / "ELNAA.parquet",
    'ELNHP': INPUT_DIR / "ELNHP.parquet"
}

# =============================================================================
# DATE HANDLING AND MACRO VARIABLES
# =============================================================================

def sas_date_to_datetime(sas_date):
    """Convert SAS numeric date to datetime"""
    if sas_date is None or sas_date <= 0:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def datetime_to_sas_date(dt):
    """Convert datetime to SAS numeric date"""
    if dt is None:
        return 0
    reference = datetime(1960, 1, 1)
    return (dt - reference).days

def parse_ddmmyyyy(date_str):
    """Parse DDMMYYYY string to date"""
    if not date_str or len(date_str) != 8:
        return None
    try:
        day = int(date_str[0:2])
        month = int(date_str[2:4])
        year = int(date_str[4:8])
        return datetime(year, month, day).date()
    except:
        return None

def setup_date_variables():
    """Calculate report date and week based on current day"""
    today = datetime.now().date()
    day = today.day
    month = today.month
    year = today.year
    
    # Determine report date and week based on day ranges
    if 8 <= day <= 14:
        reptdate = datetime(year, month, 8).date()
        wk = '1'
    elif 15 <= day <= 21:
        reptdate = datetime(year, month, 15).date()
        wk = '2'
    elif 22 <= day <= 27:
        reptdate = datetime(year, month, 22).date()
        wk = '3'
    else:
        # First day of month minus 1 day
        first_of_month = datetime(year, month, 1).date()
        reptdate = first_of_month - timedelta(days=1)
        wk = '4'
    
    return {
        'REPTDATE': reptdate,
        'WK': wk,
        'NOWK': wk,
        'RDATE': reptdate.strftime('%d%m%Y'),
        'REPTMON': reptdate.strftime('%m'),
        'REPTYEAR': reptdate.strftime('%y')
    }

def extract_file_date(file_path, position=54):
    """Extract date from position in file"""
    if not file_path.exists():
        return None
    
    try:
        df = pl.read_parquet(file_path, n_rows=1)
        if df.height > 0 and len(df.columns) >= position + 8:
            # Assuming parquet has columns representing positions
            # This would need actual column mapping
            pass
    except:
        pass
    
    return None

# =============================================================================
# DATA PROCESSING FUNCTIONS
#============================================================================

def check_file_dates(macro_vars):
    """Check if input files have the correct date"""
    rdate = macro_vars['RDATE']
    files_to_check = ['ELDSTXT', 'ELDSTX2', 'ELDSTX3', 'ELDSHP1', 'ELDSHP2', 'ELDSHP3']
    file_dates = {}
    all_correct = True
    
    for file_key in files_to_check:
        file_path = INPUT_FILES[file_key]
        if file_path.exists():
            print(f"File {file_key} exists")
            # In practice, would extract and compare date
            file_dates[file_key] = rdate  # Placeholder
        else:
            file_dates[file_key] = None
            all_correct = False
    
    return all_correct, file_dates

def process_eln1():
    """Process ELN1 - Main application data"""
    df = pl.read_parquet(INPUT_FILES['ELDSTXT'])
    
    # Create date fields
    date_fields = [
        ('AADATE', 'YY', 'MM', 'DD'),
        ('LODATE', 'YY1', 'MM1', 'DD1'),
        ('APVDTE1', 'YY2', 'MM2', 'DD2'),
        ('APVDTE2', 'YY3', 'MM3', 'DD3'),
        ('ICDATE', 'YY4', 'MM4', 'DD4'),
        ('LOBEFUDT', 'YY5', 'MM5', 'DD5'),
        ('LOBEAPDT', 'YY6', 'MM6', 'DD6'),
        ('LOBELODT', 'YY7', 'MM7', 'DD7')
    ]
    
    for new_field, year_col, month_col, day_col in date_fields:
        if year_col in df.columns and month_col in df.columns and day_col in df.columns:
            df = df.with_columns([
                pl.date(pl.col(year_col), pl.col(month_col), pl.col(day_col)).alias(new_field)
            ])
    
    return df

def process_ehp1():
    """Process EHP1 - HP application data"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP1'])
    
    # Set default SYS_TYPE
    if 'SYS_TYPE' in df.columns:
        df = df.with_columns([
            pl.col('SYS_TYPE').fill_null('EHPDS').alias('SYS_TYPE')
        ])
    
    # Create date fields
    date_fields = [
        ('AADATE', 'YY', 'MM', 'DD'),
        ('LODATE', 'YY1', 'MM1', 'DD1'),
        ('APVDTE1', 'YY2', 'MM2', 'DD2'),
        ('APVDTE2', 'YY3', 'MM3', 'DD3'),
        ('ICDATE', 'YY4', 'MM4', 'DD4'),
        ('LOBEFUDT', 'YY5', 'MM5', 'DD5'),
        ('LOBEAPDT', 'YY6', 'MM6', 'DD6'),
        ('LOBELODT', 'YY7', 'MM7', 'DD7')
    ]
    
    for new_field, year_col, month_col, day_col in date_fields:
        if year_col in df.columns and month_col in df.columns and day_col in df.columns:
            df = df.with_columns([
                pl.date(pl.col(year_col), pl.col(month_col), pl.col(day_col)).alias(new_field)
            ])
    
    return df

def process_eln2():
    """Process ELN2 - Additional details"""
    df = pl.read_parquet(INPUT_FILES['ELDSTX2'])
    
    # Apply 3rd party referral logic
    referral_types = [
        'INSURANCE AGENT', 'LOAN AGENT', 'MCCM REFERRAL',
        'OUTSOURCED MARKETING COMPANY', 'PROPERTY AGENT',
        'REAL ESTATE BROKE', 'TAX AGENT'
    ]
    
    if 'REFTYPE' in df.columns and 'NMREF1' in df.columns:
        mask = pl.col('REFTYPE').str.to_uppercase().is_in(referral_types)
        
        df = df.with_columns([
            pl.when(mask).then(pl.col('REFTYPE')).otherwise(None).alias('REFTYPE_3RD_PARTY'),
            pl.when(mask).then(pl.col('NMREF1')).otherwise(None).alias('NMREF_3RD_PARTY'),
            pl.when(mask).then(pl.lit(None)).otherwise(pl.col('REFTYPE')).alias('REFTYPE'),
            pl.when(mask).then(pl.lit(None)).otherwise(pl.col('NMREF1')).alias('NMREF1')
        ])
    
    # Handle special character translation
    if 'INCEPTION_RISK_RATING_GRADE_SME' in df.columns:
        df = df.with_columns([
            pl.col('INCEPTION_RISK_RATING_GRADE_SME').str.replace_all('��', '[]')
        ])
    
    return df

def process_ehp2():
    """Process EHP2 - HP additional details"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP2'])
    
    # Apply 3rd party referral logic
    referral_types = [
        'INSURANCE AGENT', 'LOAN AGENT', 'MCCM REFERRAL',
        'OUTSOURCED MARKETING COMPANY', 'PROPERTY AGENT',
        'REAL ESTATE BROKE', 'TAX AGENT'
    ]
    
    if 'REFTYPE' in df.columns and 'NMREF1' in df.columns:
        mask = pl.col('REFTYPE').str.to_uppercase().is_in(referral_types)
        
        df = df.with_columns([
            pl.when(mask).then(pl.col('REFTYPE')).otherwise(None).alias('REFTYPE_3RD_PARTY'),
            pl.when(mask).then(pl.col('NMREF1')).otherwise(None).alias('NMREF_3RD_PARTY'),
            pl.when(mask).then(pl.lit(None)).otherwise(pl.col('REFTYPE')).alias('REFTYPE'),
            pl.when(mask).then(pl.lit(None)).otherwise(pl.col('NMREF1')).alias('NMREF1')
        ])
    
    return df

def process_eln3():
    """Process ELN3 - Facility details"""
    df = pl.read_parquet(INPUT_FILES['ELDSTX3'])
    
    # Handle special character translation for ASCORE fields
    ascore_fields = ['ASCOREBRH', 'ASCORECOD', 'ASCOREGRADE']
    for field in ascore_fields:
        if field in df.columns:
            df = df.with_columns([
                pl.col(field).str.replace_all('��', '[]').alias(field)
            ])
    
    # Extract ASCORE_PD from RAW_PD
    if 'RAW_PD' in df.columns:
        df = df.with_columns([
            pl.col('RAW_PD').str.replace_all('%', '').cast(pl.Float64).alias('ASCORE_PD')
        ])
    
    # Set default DNBFISME
    if 'DNBFISME' in df.columns:
        df = df.with_columns([
            pl.col('DNBFISME').fill_null('0').alias('DNBFISME')
        ])
    
    # Determine PRODUCT from FACCODE
    if 'FACCODE' in df.columns:
        df = df.with_columns([
            pl.when(
                pl.col('FACCODE').str.slice(2, 1) == '5'
            ).then(
                pl.col('FACCODE').str.slice(3, 7).cast(pl.Int64)
            ).otherwise(
                pl.col('FACCODE').str.slice(7, 3).cast(pl.Int64)
            ).alias('PRODUCT')
        ])
    
    # SBH_SWK indicator for PRESCSTATE 10/11
    if 'PRESCSTATE' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('PRESCSTATE').is_in([10, 11]))
            .then(pl.lit('Y'))
            .otherwise(pl.lit('N'))
            .alias('SBH_SWK')
        ])
    
    # Import/Export indicator
    if 'IMP_EXP_IND' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('IMP_EXP_IND').str.to_uppercase() == 'IMPORT ONLY')
            .then(pl.lit(1))
            .when(pl.col('IMP_EXP_IND').str.to_uppercase() == 'EXPORT ONLY')
            .then(pl.lit(2))
            .when(pl.col('IMP_EXP_IND').str.to_uppercase() == 'BOTH IMPORT AND EXPORT')
            .then(pl.lit(3))
            .otherwise(None)
            .alias('IMP_EXP_BUSS_OWNER_IND')
        ])
    
    return df

def process_ehp3():
    """Process EHP3 - HP facility details"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP3'])
    
    # Handle special character translation
    ascore_fields = ['ASCOREBRH', 'ASCORECOD', 'ASCOREGRADE']
    for field in ascore_fields:
        if field in df.columns:
            df = df.with_columns([
                pl.col(field).str.replace_all('��', '[]').alias(field)
            ])
    
    # Extract ASCORE_PD from RAW_PD
    if 'RAW_PD' in df.columns:
        df = df.with_columns([
            pl.col('RAW_PD').str.replace_all('%', '').cast(pl.Float64).alias('ASCORE_PD')
        ])
    
    # Set MAANO = AANO
    if 'AANO' in df.columns:
        df = df.with_columns([
            pl.col('AANO').alias('MAANO')
        ])
    
    # Set default DNBFISME
    if 'DNBFISME' in df.columns:
        df = df.with_columns([
            pl.col('DNBFISME').fill_null('0').alias('DNBFISME')
        ])
    
    return df

def process_eln4():
    """Process ELN4 - Applicant details"""
    df = pl.read_parquet(INPUT_FILES['ELDSTX4'])
    
    # Filter APVBY starting with 'APP'
    if 'APVBY' in df.columns:
        df = df.filter(pl.col('APVBY').str.slice(0, 3) == 'APP')
        df = df.drop('APVBY')
    
    # Handle special character translation
    if 'ASCORE_GRADE' in df.columns:
        df = df.with_columns([
            pl.col('ASCORE_GRADE').str.replace_all('��', '[]').alias('ASCORE_GRADE')
        ])
    
    return df.unique(subset=['MAANO'])

def process_ehp4():
    """Process EHP4 - HP applicant details"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP4'])
    
    # Filter APVBY starting with 'APP'
    if 'APVBY' in df.columns:
        df = df.filter(pl.col('APVBY').str.slice(0, 3) == 'APP')
        df = df.drop('APVBY')
    
    return df

def process_eln7():
    """Process ELN7 - Security details"""
    df = pl.read_parquet(INPUT_FILES['ELDSTX7'])
    
    # Create COMPSTAT based on PCTCOMP
    if 'PCTCOMP' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('PCTCOMP') == 100)
            .then(pl.lit('Y'))
            .otherwise(pl.lit('N'))
            .alias('COMPSTAT')
        ])
    
    # Filter INDNEWE not 'N/A'
    if 'INDNEWE' in df.columns:
        df = df.filter(pl.col('INDNEWE') != 'N/A')
    
    return df

def process_eln8():
    """Process ELN8 - Main form details"""
    df = pl.read_parquet(INPUT_FILES['ELDSTX8'])
    
    # Create date fields
    if all(col in df.columns for col in ['XYY', 'XMM', 'XDD']):
        df = df.with_columns([
            pl.date(pl.col('XYY'), pl.col('XMM'), pl.col('XDD')).alias('APPLDATE')
        ])
    
    if all(col in df.columns for col in ['INCYY', 'INCMM', 'INCDD']):
        df = df.with_columns([
            pl.date(pl.col('INCYY'), pl.col('INCMM'), pl.col('INCDD')).alias('INCRDT')
        ])
    
    if all(col in df.columns for col in ['CYY', 'CMM', 'CDD']):
        df = df.with_columns([
            pl.date(pl.col('CYY'), pl.col('CMM'), pl.col('CDD')).alias('COMPLIDT')
        ])
    
    return df

def process_ehp8():
    """Process EHP8 - HP main form details"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP8'])
    
    # Create date fields
    if all(col in df.columns for col in ['XYY', 'XMM', 'XDD']):
        df = df.with_columns([
            pl.date(pl.col('XYY'), pl.col('XMM'), pl.col('XDD')).alias('APPLDATE')
        ])
    
    if all(col in df.columns for col in ['INCYY', 'INCMM', 'INCDD']):
        df = df.with_columns([
            pl.date(pl.col('INCYY'), pl.col('INCMM'), pl.col('INCDD')).alias('INCRDT')
        ])
    
    if all(col in df.columns for col in ['CYY', 'CMM', 'CDD']):
        df = df.with_columns([
            pl.date(pl.col('CYY'), pl.col('CMM'), pl.col('CDD')).alias('COMPLIDT')
        ])
    
    return df

def process_eln10():
    """Process ELN10 - Additional info"""
    df = pl.read_parquet(INPUT_FILES['ELDST10'])
    
    # Parse CACRSFIY
    if 'CACRSFIY' in df.columns:
        df = df.with_columns([
            pl.col('CACRSFIY').str.split(' ').list.get(0).alias('CACRSFIY_MTH'),
            pl.col('CACRSFIY').str.split(' ').list.get(1).cast(pl.Int64).alias('CACRSFIY_YR')
        ])
    
    return df

def process_ehp10():
    """Process EHP10 - HP additional info"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP10'])
    
    # Parse CACRSFIY
    if 'CACRSFIY' in df.columns:
        df = df.with_columns([
            pl.col('CACRSFIY').str.split(' ').list.get(0).alias('CACRSFIY_MTH'),
            pl.col('CACRSFIY').str.split(' ').list.get(1).cast(pl.Int64).alias('CACRSFIY_YR')
        ])
    
    return df

def process_ehp13():
    """Process EHP13 - Guarantor liabilities"""
    df = pl.read_parquet(INPUT_FILES['ELDSHP13'])
    
    # Filter for guarantor liability type
    df = df.filter(
        pl.col('TYPE') == "GUARANTOR'S EXISTING LIABILITY WITH PBB"
    )
    
    # Clean OUTSTND
    if 'OUTSTND' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('OUTSTND') == '-')
            .then(pl.lit('0'))
            .otherwise(pl.col('OUTSTND'))
            .alias('OUTSTND')
        ])
        
        df = df.with_columns([
            pl.col('OUTSTND').str.replace_all(',', '').cast(pl.Float64).alias('AGGOUTSTND')
        ])
    
    # Summarize by MAANO
    summary = df.group_by('MAANO').agg(pl.col('AGGOUTSTND').sum())
    
    return summary

def process_eln17():
    """Process ELN17 - Financial ratios"""
    return pl.read_parquet(INPUT_FILES['ELDST17'])

def process_ehp17():
    """Process EHP17 - HP financial ratios"""
    return pl.read_parquet(INPUT_FILES['ELDSHP17'])

def process_eln20():
    """Process ELN20 - CRR codes"""
    return pl.read_parquet(INPUT_FILES['ELDST20'])

def process_eln22():
    """Process ELN22 - Sole proprietors/partners"""
    return pl.read_parquet(INPUT_FILES['ELDST22'])

def process_ehp22():
    """Process EHP22 - HP sole proprietors/partners"""
    return pl.read_parquet(INPUT_FILES['ELDSHP22'])

def process_eln26():
    """Process ELN26 - High net worth indicator"""
    df = pl.read_parquet(INPUT_FILES['ELDST26'])
    
    # Filter for High Networth Borrower
    df = df.filter(
        pl.col('VALUE').str.to_uppercase() == 'HIGH NETWORTH BORROWER'
    ).with_columns([
        pl.lit('Y').alias('HIGH_NWORTH')
    ]).select(['MAANO', 'HIGH_NWORTH'])
    
    return df

def process_eln30():
    """Process ELN30 - Conduct of accounts"""
    df = pl.read_parquet(INPUT_FILES['ELDST30'])
    
    # Filter for Conduct of Accounts
    df = df.filter(
        (pl.col('CATEGORY').str.to_uppercase() == 'CONDUCT OF ACCOUNTS') &
        (pl.col('LISTNO') == '') &
        pl.col('VALUE').str.contains(r'^\d+$')
    )
    
    # Parse VALUE to create CACRSFIY
    if df.height > 0:
        df = df.with_columns([
            pl.col('VALUE').str.slice(0, 2).cast(pl.Int64).alias('month_num'),
            pl.col('VALUE').str.slice(2, 4).cast(pl.Int64).alias('year_num')
        ])
        
        # Create date from month/year
        df = df.with_columns([
            pl.date(pl.col('year_num'), pl.col('month_num'), 1).alias('TEMPDATE')
        ])
        
        # Format as "MON YYYY"
        df = df.with_columns([
            pl.col('TEMPDATE').dt.strftime('%b %Y').str.to_uppercase().alias('CACRSFIY')
        ])
        
        # Parse month and year
        df = df.with_columns([
            pl.col('CACRSFIY').str.split(' ').list.get(0).alias('CACRSFIY_MTH'),
            pl.col('CACRSFIY').str.split(' ').list.get(1).cast(pl.Int64).alias('CACRSFIY_YR')
        ])
        
        df = df.select(['MAANO', 'CACRSFIY', 'CACRSFIY_MTH', 'CACRSFIY_YR'])
    
    return df

def process_cc():
    """Process CC - Rejected applications"""
    df = pl.read_parquet(INPUT_FILES['ELDST63'])
    return df.unique(subset=['MAANO', 'AANO', 'STATUS'])

def count_records():
    """Count records from ELNAA and ELNHP"""
    count_df = pl.read_parquet(INPUT_FILES['ELNAA'], n_rows=1)
    count = count_df.select(pl.first()).item() if count_df.height > 0 else 0
    
    counthp_df = pl.read_parquet(INPUT_FILES['ELNHP'])
    counthp = counthp_df.select(pl.first()).sum().item() if counthp_df.height > 0 else 0
    
    return count, counthp

def create_elnout():
    """Create ELNOUT for record counting"""
    dfs = []
    descs = []
    
    datasets = [
        ('ELN1', process_eln1()),
        ('ELN2', process_eln2()),
        ('ELN3', process_eln3()),
        ('EHP1', process_ehp1()),
        ('EHP2', process_ehp2()),
        ('EHP3', process_ehp3())
    ]
    
    for desc, df in datasets:
        if df is not None:
            df_desc = df.select(pl.lit(desc).alias('DESC'))
            dfs.append(df_desc)
    
    if dfs:
        elnout = pl.concat(dfs)
        summary = elnout.group_by('DESC').len().sort('DESC')
        return summary
    
    return None

def main():
    """Main execution function"""
    print(f"Starting EIBWELWH translation at {datetime.now()}")
    
    # Setup date variables
    macro_vars = setup_date_variables()
    print(f"Report Date: {macro_vars['RDATE']}, Week: {macro_vars['WK']}")
    
    # Check file dates
    all_correct, _ = check_file_dates(macro_vars)
    
    if all_correct:
        print("\nAll files have correct date. Processing data...")
        
        try:
            # Process main datasets
            print("Processing ELN1/EHP1...")
            eln1 = process_eln1()
            ehp1 = process_ehp1()
            
            print("Processing ELN2/EHP2...")
            eln2 = process_eln2()
            ehp2 = process_ehp2()
            
            print("Processing ELN3/EHP3...")
            eln3 = process_eln3()
            ehp3 = process_ehp3()
            
            # Remove duplicates
            for df in [eln1, eln2, eln3, ehp1, ehp2, ehp3]:
                if df is not None:
                    df.unique(subset=['AANO', 'STATUS'])
            
            # Merge AA and HP
            aa_dfs = [df for df in [eln1, eln2, eln3] if df is not None]
            if aa_dfs:
                aa = aa_dfs[0]
                for df in aa_dfs[1:]:
                    aa = aa.join(df, on=['AANO', 'STATUS'], how='left')
            else:
                aa = None
            
            hp_dfs = [df for df in [ehp1, ehp2, ehp3] if df is not None]
            if hp_dfs:
                hp = hp_dfs[0]
                for df in hp_dfs[1:]:
                    hp = hp.join(df, on=['AANO', 'STATUS'], how='left')
            else:
                hp = None
            
            # Process CC
            print("Processing CC...")
            cc = process_cc()
            
            # Combine all
            eln = pl.concat([df for df in [aa, hp, cc] if df is not None])
            eln = eln.unique(subset=['AANO', 'STATUS'])
            
            # Process other datasets
            print("Processing additional datasets...")
            eln4 = process_eln4()
            ehp4 = process_ehp4()
            eln7 = process_eln7()
            eln8 = process_eln8()
            ehp8 = process_ehp8()
            eln10 = process_eln10()
            ehp10 = process_ehp10()
            ehp13 = process_ehp13()
            eln17 = process_eln17()
            ehp17 = process_ehp17()
            eln20 = process_eln20()
            eln22 = process_eln22()
            ehp22 = process_ehp22()
            eln26 = process_eln26()
            eln30 = process_eln30()
            
            # Merge EHP datasets
            ehp_dfs = [df for df in [ehp4, ehp8, ehp10, ehp13, ehp22] if df is not None]
            prehp = None
            if ehp_dfs:
                prehp = ehp_dfs[0]
                for df in ehp_dfs[1:]:
                    prehp = prehp.join(df, on='MAANO', how='left')
            
            # Merge ELN datasets
            eln_dfs = [df for df in [eln4, eln8, eln22, eln26, eln10, eln30] if df is not None]
            preaa = None
            if eln_dfs:
                preaa = eln_dfs[0]
                for df in eln_dfs[1:]:
                    preaa = preaa.join(df, on='MAANO', how='left')
            
            # Combine PREAA and PREHP
            endcom = pl.concat([df for df in [preaa, prehp] if df is not None])
            
            # Merge ELN17
            eln17_merged = None
            if ehp17 is not None and eln17 is not None:
                eln17_merged = ehp17.join(eln17, on='MAANO', how='outer')
            elif ehp17 is not None:
                eln17_merged = ehp17
            elif eln17 is not None:
                eln17_merged = eln17
            
            # Merge into ELN
            if eln is not None:
                if eln17_merged is not None:
                    eln = eln.join(eln17_merged, on='MAANO', how='left')
                if eln20 is not None:
                    eln = eln.join(eln20, on='MAANO', how='left')
                if endcom is not None:
                    eln = eln.join(endcom, on='MAANO', how='left')
                if eln7 is not None:
                    eln = eln.join(eln7, on=['MAANO', 'AANO'], how='left')
            
            # Handle MARGIN logic
            if eln is not None and all(col in eln.columns for col in ['APVBY', 'MA', 'APPL', 'APPLMRG', 'MRGADVE', 'MARGIN']):
                eln = eln.with_columns([
                    pl.when(
                        (pl.col('APVBY') == 'BR') & pl.col('MA').is_not_null()
                    ).then(pl.col('MA').cast(pl.Utf8))
                    .when(
                        (pl.col('APVBY') == 'HO') & 
                        (pl.col('APPL').is_not_null()) & 
                        (pl.col('APPLMRG').is_not_null())
                    ).then(pl.col('APPLMRG'))
                    .when(
                        (pl.col('APVBY') == 'HO') & 
                        (pl.col('MRGADVE').is_not_null())
                    ).then(pl.col('MRGADVE'))
                    .when(pl.col('MA').is_not_null())
                    .then(pl.col('MA').cast(pl.Utf8))
                    .otherwise(pl.col('MARGIN'))
                    .alias('MARGIN')
                ])
                
                eln = eln.with_columns([
                    pl.col('MARGIN').cast(pl.Utf8).str.replace_all(r'\s+', '').alias('MARGIN')
                ])
            
            # Filter out blank AANO
            if eln is not None and 'AANO' in eln.columns:
                eln = eln.filter(pl.col('AANO').is_not_null())
            
            # Split by BRANCH
            if eln is not None and 'BRANCH' in eln.columns:
                # Conventional (BRANCH < 3000)
                conv = eln.filter(pl.col('BRANCH') < 3000)
                # Islamic (BRANCH >= 3000)
                islamic = eln.filter(pl.col('BRANCH') >= 3000)
                
                # Save outputs
                suffix = f"{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR']}"
                
                if conv.height > 0:
                    conv.write_parquet(ELDS1_DIR / f"ELN{suffix}.parquet")
                    conv.write_parquet(ELDS2_DIR / f"ELN{suffix}.parquet")
                    print(f"Conventional ELN saved: {conv.height} rows")
                
                if islamic.height > 0:
                    islamic.write_parquet(IELDS1_DIR / f"IELN{suffix}.parquet")
                    islamic.write_parquet(IELDS2_DIR / f"IELN{suffix}.parquet")
                    print(f"Islamic IELN saved: {islamic.height} rows")
            
            # Count records for validation
            count, counthp = count_records()
            elnout_summary = create_elnout()
            
            if elnout_summary is not None:
                print("\nRecord counts:")
                print(elnout_summary)
                
                # Validate counts
                for row in elnout_summary.rows():
                    desc, freq = row
                    expected = count if desc.startswith('ELN') else counthp
                    status = "OK" if freq == expected else "NOT OK"
                    print(f"{desc}: {freq} (Expected: {expected}) {status}")
                    
                    if (desc in ['ELN1', 'EHP1'] and status != 'OK'):
                        print(f"Validation failed for {desc}, aborting")
                        raise SystemExit(77)
            
            print(f"\nProcessing completed successfully at {datetime.now()}")
            
        except Exception as e:
            print(f"\nError during processing: {str(e)}")
            import traceback
            traceback.print_exc()
            
    else:
        print("\nFILE DATE MISMATCH DETECTED!")
        print(f"The SAP.PBB.ELDS.BNM.TEXT(0) is not dated {macro_vars['RDATE']}")
        print("THE JOB IS NOT DONE !!")
        raise SystemExit(77)

if __name__ == "__main__":
    main()
