"""
EIBWFCFD - Foreign Currency Fixed Deposit Processing and Export
Extracts FCY FD data, merges with additional sources, processes dates,
and exports to SAS transport format (XPORT).
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import pandas as pd
from sas7bdat import SAS7BDAT  # Requires sas7bdat package
import xport  # Requires xport package

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'DEPOSIT': 'data/deposit/',
    'MNIFD': 'data/mnifd/',
    'SIGNA': 'data/signa/',
    'MISMTD': 'data/mismtd/',
    'PGM': 'pgm/',
    'TEMP': 'temp/',
    'DEPO': 'data/depo/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# FCY Products (from macro &FCY)
FCY_PRODUCTS = [110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 
                170, 175, 180, 185, 190, 195, 200, 205, 210, 215, 220, 225, 
                230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 
                290, 295, 300, 305, 310, 315, 320, 325, 330, 335, 340, 345, 
                350, 355, 360, 365, 370, 375, 380, 385, 390, 395, 400]

# FDVAR macro (variables to keep)
FDVAR = ['ACCTNO', 'BRANCH', 'PRODUCT', 'NAME', 'OPENDT', 'CLOSEDT', 'CUSTCODE',
         'INTYTD', 'PURPOSE', 'LEDGBAL', 'OPENIND', 'INTPD', 'INTPLAN', 'CURBAL',
         'STATE', 'CURCODE', 'BDATE', 'MTDAVBAL', 'FORATE', 'FORBAL', 'CURBALUS',
         'SECOND', 'USER3', 'DNBFISME', 'MTDAVBAL_MIS', 'MTDAVFORBAL_MIS',
         'ACSTATUS4', 'ACSTATUS5', 'ACSTATUS6', 'ACSTATUS7', 'ACSTATUS8',
         'ACSTATUS9', 'ACSTATUS10', 'POST_IND', 'REASON', 'INSTRUCTIONS',
         'DTLSTCUST', 'CLASSIFI', 'REOPENDT', 'ESIGNATURE', 'STMT_CYCLE',
         'NXT_STMT_CYCLE_DT']

# =============================================================================
# FORMAT INCLUSION
# =============================================================================
def include_pbbdpfmt():
    """Equivalent to %INC PGM(PBBDPFMT) - load PURPOSEM format"""
    formats = {}
    try:
        fmt_file = Path(PATHS['PGM']) / "PBBDPFMT.sas"
        if fmt_file.exists():
            with open(fmt_file, 'r') as f:
                content = f.read()
                # Look for PURPOSEM format
                import re
                pattern = r'VALUE\s+PURPOSEM\s+(.*?);'
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    fmt_text = match.group(1)
                    for line in fmt_text.split('\n'):
                        if '=' in line:
                            parts = line.split('=')
                            if len(parts) == 2:
                                code = parts[0].strip()
                                label = parts[1].strip().strip("'").strip('"')
                                try:
                                    formats[int(code)] = label
                                except:
                                    formats[code] = label
    except Exception as e:
        print(f"  Warning: Could not load PURPOSEM format: {e}")
    
    return formats

# =============================================================================
# REPORT DATE PROCESSING
# =============================================================================
def get_report_vars():
    """Get report date and macro variables from DEPOSIT.REPTDATE"""
    df = pl.read_parquet(f"{PATHS['DEPOSIT']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    return {
        'reptyear': f"{reptdate.year % 100:02d}",
        'reptmon': f"{reptdate.month:02d}",
        'reptday': f"{reptdate.day:02d}",
        'nowk': nowk,
        'rdate': reptdate.strftime('%d%m%Y'),
        'reptdate': reptdate
    }

# =============================================================================
# DATE CONVERSION FUNCTION
# =============================================================================
def convert_sas_date(date_val):
    """Convert SAS numeric date (days since 1960) to Python date"""
    if date_val in (0, None) or date_val == '':
        return None
    try:
        # Handle packed decimal stored as int
        date_str = f"{int(float(date_val)):011d}"[:8]
        if len(date_str) == 8:
            return datetime(int(date_str[4:8]), int(date_str[0:2]), int(date_str[2:4])).date()
    except:
        pass
    return None

# =============================================================================
# SAS TRANSPORT FILE CREATION
# =============================================================================
def create_sas_transport(df, filename, dataset_name):
    """Create SAS transport file (XPORT format)"""
    # Convert Polars to Pandas
    pdf = df.to_pandas()
    
    # Write XPORT file
    with open(filename, 'wb') as f:
        writer = xport.XportWriter(f)
        writer.write(pdf, dataset_name)
        writer.close()
    
    print(f"  SAS transport file created: {filename}")

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIBWFCFD - Foreign Currency FD Processing and Export")
    print("=" * 60)
    
    # Include PBBDPFMT formats
    print("\nIncluding PBBDPFMT...")
    formats = include_pbbdpfmt()
    print(f"  Loaded {len(formats)} PURPOSEM formats")
    
    # Get report variables
    rep = get_report_vars()
    print(f"\nReport Date: {rep['reptdate'].strftime('%d/%m/%Y')}")
    print(f"NOWK: {rep['nowk']}, Month: {rep['reptmon']}, Year: {rep['reptyear']}")
    
    # Create REPTDATE dataset
    rept_df = pl.DataFrame({'REPTDATE': [rep['reptdate']]})
    rept_df.write_parquet(f"{PATHS['DEPO']}REPTDATE.parquet")
    
    # Read and sort FD data
    print("\nReading FD data...")
    fd_df = pl.read_parquet(f"{PATHS['DEPOSIT']}FD.parquet")
    fd_df = fd_df.rename({'PURPOSE': 'CLASSIFI'})
    fd_df = fd_df.sort('ACCTNO')
    
    # Create FD dataset
    print("Processing FD records...")
    fd = fd_df.with_columns([
        pl.col('SECTOR').alias('PURPOSE'),
        pl.col('SECTOR').alias('PURPOSEM'),
        pl.col('INTPD').alias('INTYTD')
    ]).filter(pl.col('PRODUCT').is_in(FCY_PRODUCTS))
    
    # Apply PURPOSEM format
    if formats:
        fd = fd.with_columns([
            pl.col('PURPOSEM').map_elements(
                lambda x: formats.get(x, str(x)), return_dtype=pl.Utf8
            ).alias('PURPOSEM_FMT')
        ])
    
    # Read MNIFD.FD and summarize
    print("Processing MNIFD.FD...")
    fdcd_df = pl.read_parquet(f"{PATHS['MNIFD']}FD.parquet").sort('ACCTNO')
    fdcd_sum = fdcd_df.group_by('ACCTNO').agg([
        pl.col('INTPAY').sum().alias('INTPD')
    ])
    
    # Read SIGNA.SMSACC
    print("Processing SIGNA.SMSACC...")
    try:
        saacc_df = pl.read_parquet(f"{PATHS['SIGNA']}SMSACC.parquet").sort('ACCTNO')
    except:
        saacc_df = pl.DataFrame(schema={'ACCTNO': pl.Int64})
    
    # Read MISMTD.FAVG
    try:
        favg_df = pl.read_parquet(f"{PATHS['MISMTD']}FAVG{rep['reptmon']}.parquet").sort('ACCTNO')
    except:
        favg_df = pl.DataFrame(schema={'ACCTNO': pl.Int64})
    
    # Merge all datasets
    print("Merging datasets...")
    fcyfd = fd.join(fdcd_sum, on='ACCTNO', how='left')
    fcyfd = fcyfd.join(saacc_df, on='ACCTNO', how='left')
    fcyfd = fcyfd.join(favg_df, on='ACCTNO', how='left')
    
    # Process dates and fields
    print("Processing date fields...")
    
    # Rename PSREASON to REASON
    if 'PSREASON' in fcyfd.columns:
        fcyfd = fcyfd.rename({'PSREASON': 'REASON'})
    
    # Extract ACSTATUS fields from STATCD
    if 'STATCD' in fcyfd.columns:
        for i in range(4, 11):
            fcyfd = fcyfd.with_columns([
                pl.col('STATCD').str.slice(i-1, 1).alias(f'ACSTATUS{i}')
            ])
    
    # Convert date fields
    date_fields = ['OPENDT', 'REOPENDT', 'CLOSEDT', 'DTLSTCUST']
    for field in date_fields:
        if field in fcyfd.columns:
            fcyfd = fcyfd.with_columns([
                pl.col(field).map_elements(convert_sas_date, return_dtype=pl.Date).alias(f'{field}_NEW')
            ])
            fcyfd = fcyfd.drop(field).rename({f'{field}_NEW': field})
    
    # Handle ESIGNATURE
    if 'ESIGNATURE' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.when(pl.col('ESIGNATURE').is_null())
            .then(pl.lit('N'))
            .otherwise(pl.col('ESIGNATURE')).alias('ESIGNATURE')
        ])
    
    # Keep only FDVAR columns
    keep_cols = [c for c in FDVAR if c in fcyfd.columns]
    fcyfd = fcyfd.select(keep_cols)
    
    # Split into two datasets (DATA step with two outputs)
    print("\nSplitting datasets...")
    depo_df = fcyfd.clone()
    temp_df = fcyfd.clone()
    
    # Save DEPO dataset
    depo_filename = f"FCYFD{rep['reptmon']}{rep['nowk']}{rep['reptyear']}.parquet"
    depo_path = f"{PATHS['DEPO']}{depo_filename}"
    depo_df.write_parquet(depo_path)
    print(f"  DEPO dataset: {depo_path}")
    
    # Save TEMP dataset (for transport)
    temp_path = f"{PATHS['TEMP']}FCYFD{rep['reptmon']}{rep['nowk']}{rep['reptyear']}.parquet"
    temp_df.write_parquet(temp_path)
    print(f"  TEMP dataset: {temp_path}")
    
    # Export to SAS transport format (PROC CPORT equivalent)
    print("\nExporting to SAS transport format (PROC CPORT)...")
    transport_file = PATHS['OUTPUT'] / "SAP.PBB.FCYFD.FCYFDFTP"
    
    try:
        # Create SAS transport file (XPORT format)
        create_sas_transport(temp_df, transport_file, f"FCYFD{rep['reptmon']}{rep['nowk']}")
        print(f"  Transport file created with DISP=OLD: {transport_file}")
    except Exception as e:
        print(f"  Error creating transport file: {e}")
        print("  Falling back to parquet copy")
        shutil.copy2(temp_path, transport_file)
    
    # Print sample data (like PROC PRINT)
    print("\n" + "=" * 60)
    print("PROC PRINT DATA=DEPO.FCYFD (first 20 records)")
    print("=" * 60)
    print(depo_df.head(20))
    
    print("\n" + "=" * 60)
    print("PROC PRINT DATA=REPTDATE")
    print("=" * 60)
    print(rept_df.with_columns([
        pl.col('REPTDATE').dt.strftime('%m/%Y').alias('MMWYY')
    ]))
    
    print("\n" + "=" * 60)
    print(f"✓ EIBWFCFD Complete")
    print(f"  Records processed: {len(fcyfd):,}")
    print(f"  Transport file: {transport_file}")

if __name__ == "__main__":
    main()
