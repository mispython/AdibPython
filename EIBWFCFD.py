"""
EIBWFCFD - Foreign Currency Fixed Deposit Processing
Extracts FCY FD data, merges with additional sources, processes dates,
and creates DEPO and TEMP datasets.
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import re

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
FDVAR = [
    'ACCTNO', 'BRANCH', 'PRODUCT', 'NAME', 'OPENDT', 'CLOSEDT', 'CUSTCODE',
    'INTYTD', 'PURPOSE', 'LEDGBAL', 'OPENIND', 'INTPD', 'INTPLAN', 'CURBAL',
    'STATE', 'CURCODE', 'BDATE', 'MTDAVBAL', 'FORATE', 'FORBAL', 'CURBALUS',
    'SECOND', 'USER3', 'DNBFISME', 'MTDAVBAL_MIS', 'MTDAVFORBAL_MIS',
    'ACSTATUS4', 'ACSTATUS5', 'ACSTATUS6', 'ACSTATUS7', 'ACSTATUS8',
    'ACSTATUS9', 'ACSTATUS10', 'POST_IND', 'REASON', 'INSTRUCTIONS',
    'DTLSTCUST', 'CLASSIFI', 'REOPENDT', 'ESIGNATURE', 'STMT_CYCLE',
    'NXT_STMT_CYCLE_DT'
]

# =============================================================================
# FORMAT INCLUSION - PBBDPFMT
# =============================================================================
def include_pbbdpfmt():
    """Equivalent to %INC PGM(PBBDPFMT) - load PURPOSEM format"""
    formats = {}
    try:
        fmt_file = Path(PATHS['PGM']) / "PBBDPFMT.sas"
        if fmt_file.exists():
            with open(fmt_file, 'r') as f:
                content = f.read()
                # Look for PURPOSEM format (2-digit numeric format)
                pattern = r'VALUE\s+PURPOSEM\s+(.*?);'
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    fmt_text = match.group(1)
                    for line in fmt_text.split('\n'):
                        line = line.strip()
                        if '=' in line:
                            parts = line.split('=')
                            if len(parts) == 2:
                                code = parts[0].strip()
                                label = parts[1].strip().strip("'").strip('"')
                                try:
                                    formats[int(code)] = label
                                except:
                                    formats[code] = label
                    print(f"  Loaded {len(formats)} PURPOSEM formats")
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
def convert_sas_date(date_val, length=8):
    """
    Convert SAS numeric date to Python date
    SAS stores dates as: MMDDYYYY in packed decimal
    """
    if date_val in (0, None) or date_val == '':
        return None
    try:
        # Handle packed decimal stored as int
        if length == 8:
            date_str = f"{int(float(date_val)):011d}"[:8]
        else:  # length 6 for MMDDYY
            date_str = f"{int(float(date_val)):09d}"[:6]
        
        if len(date_str) == 8:
            return datetime(int(date_str[4:8]), int(date_str[0:2]), int(date_str[2:4])).date()
        elif len(date_str) == 6:
            year = 2000 + int(date_str[4:6]) if int(date_str[4:6]) < 90 else 1900 + int(date_str[4:6])
            return datetime(year, int(date_str[0:2]), int(date_str[2:4])).date()
    except:
        pass
    return None

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIBWFCFD - Foreign Currency FD Processing")
    print("=" * 60)
    
    # Include PBBDPFMT formats
    print("\nIncluding PBBDPFMT...")
    formats = include_pbbdpfmt()
    
    # Get report variables
    rep = get_report_vars()
    print(f"\nReport Date: {rep['reptdate'].strftime('%d/%m/%Y')}")
    print(f"NOWK: {rep['nowk']}, Month: {rep['reptmon']}, Year: {rep['reptyear']}")
    
    # Create REPTDATE dataset (KEEP=REPTDATE)
    rept_df = pl.DataFrame({'REPTDATE': [rep['reptdate']]})
    rept_df.write_parquet(f"{PATHS['DEPO']}REPTDATE.parquet")
    print(f"\nREPTDATE saved")
    
    # Read DEPOSIT.FD, rename PURPOSE to CLASSIFI, sort by ACCTNO
    print("\nReading DEPOSIT.FD...")
    fd_df = pl.read_parquet(f"{PATHS['DEPOSIT']}FD.parquet")
    fd_df = fd_df.rename({'PURPOSE': 'CLASSIFI'})
    fd_df = fd_df.sort('ACCTNO')
    print(f"  Records: {len(fd_df):,}")
    
    # Create FD dataset with FORMAT PURPOSE PURPOSEM 2. and DROP INTPD
    print("\nCreating FD dataset...")
    fd = fd_df.with_columns([
        pl.col('SECTOR').alias('PURPOSE'),
        pl.col('SECTOR').alias('PURPOSEM'),
        pl.col('INTPD').alias('INTYTD')
    ]).filter(pl.col('PRODUCT').is_in(FCY_PRODUCTS))
    
    # Apply PURPOSEM format if available
    if formats:
        fd = fd.with_columns([
            pl.col('PURPOSEM').map_elements(
                lambda x: formats.get(x, str(x)), return_dtype=pl.Utf8
            ).alias('PURPOSEM_FMT')
        ])
    
    # Drop INTPD (as per DROP statement)
    if 'INTPD' in fd.columns:
        fd = fd.drop('INTPD')
    
    print(f"  FCY records: {len(fd):,}")
    
    # Process MNIFD.FD
    print("\nProcessing MNIFD.FD...")
    fdcd_df = pl.read_parquet(f"{PATHS['MNIFD']}FD.parquet")
    fdcd_df = fdcd_df.sort('ACCTNO')
    
    # PROC SUMMARY - sum INTPAY by ACCTNO
    fdcd_sum = fdcd_df.group_by('ACCTNO').agg([
        pl.col('INTPAY').sum().alias('INTPD')
    ])
    print(f"  MNIFD records: {len(fdcd_sum):,}")
    
    # Process SIGNA.SMSACC
    print("\nProcessing SIGNA.SMSACC...")
    try:
        saacc_df = pl.read_parquet(f"{PATHS['SIGNA']}SMSACC.parquet")
        saacc_df = saacc_df.sort('ACCTNO')
        print(f"  SIGNA records: {len(saacc_df):,}")
    except Exception as e:
        print(f"  Warning: Could not read SIGNA.SMSACC: {e}")
        saacc_df = pl.DataFrame(schema={'ACCTNO': pl.Int64})
    
    # Process MISMTD.FAVG&REPTMON
    print(f"\nProcessing MISMTD.FAVG{rep['reptmon']}...")
    try:
        favg_df = pl.read_parquet(f"{PATHS['MISMTD']}FAVG{rep['reptmon']}.parquet")
        favg_df = favg_df.sort('ACCTNO')
        print(f"  FAVG records: {len(favg_df):,}")
    except Exception as e:
        print(f"  Warning: Could not read FAVG: {e}")
        favg_df = pl.DataFrame(schema={'ACCTNO': pl.Int64})
    
    # Merge all datasets (FD (IN=A) + FDCD + SAACC + FAVG)
    print("\nMerging datasets...")
    fcyfd = fd.join(fdcd_sum, on='ACCTNO', how='left')
    fcyfd = fcyfd.join(saacc_df, on='ACCTNO', how='left')
    fcyfd = fcyfd.join(favg_df, on='ACCTNO', how='left')
    
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
    print("\nConverting date fields...")
    
    # OPENDT conversion
    if 'OPENDT' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.col('OPENDT').map_elements(
                lambda x: convert_sas_date(x, 8), return_dtype=pl.Date
            ).alias('OPENDT_NEW')
        ]).drop('OPENDT').rename({'OPENDT_NEW': 'OPENDT'})
    
    # REOPENDT conversion
    if 'REOPENDT' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.col('REOPENDT').map_elements(
                lambda x: convert_sas_date(x, 8), return_dtype=pl.Date
            ).alias('REOPENDT_NEW')
        ]).drop('REOPENDT').rename({'REOPENDT_NEW': 'REOPENDT'})
    
    # CLOSEDT conversion
    if 'CLOSEDT' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.col('CLOSEDT').map_elements(
                lambda x: convert_sas_date(x, 8), return_dtype=pl.Date
            ).alias('CLOSEDT_NEW')
        ]).drop('CLOSEDT').rename({'CLOSEDT_NEW': 'CLOSEDT'})
    
    # DTLSTCUST conversion (6-digit date)
    if 'DTLSTCUST' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.col('DTLSTCUST').map_elements(
                lambda x: convert_sas_date(x, 6), return_dtype=pl.Date
            ).alias('DTLSTCUST_NEW')
        ]).drop('DTLSTCUST').rename({'DTLSTCUST_NEW': 'DTLSTCUST'})
    
    # Handle ESIGNATURE default
    if 'ESIGNATURE' in fcyfd.columns:
        fcyfd = fcyfd.with_columns([
            pl.when(pl.col('ESIGNATURE').is_null() | (pl.col('ESIGNATURE') == ''))
            .then(pl.lit('N'))
            .otherwise(pl.col('ESIGNATURE')).alias('ESIGNATURE')
        ])
    
    # Keep only FDVAR columns (ACSTATUS4-ACSTATUS10 are already created)
    print("\nApplying FDVAR KEEP list...")
    keep_cols = [c for c in FDVAR if c in fcyfd.columns]
    fcyfd = fcyfd.select(keep_cols)
    print(f"  Keeping {len(keep_cols)} columns")
    
    # Create DEPO and TEMP datasets (DATA step with two outputs)
    print("\nCreating output datasets...")
    
    # DEPO dataset
    depo_filename = f"FCYFD{rep['reptmon']}{rep['nowk']}{rep['reptyear']}.parquet"
    depo_path = f"{PATHS['DEPO']}{depo_filename}"
    fcyfd.write_parquet(depo_path)
    print(f"  DEPO dataset: {depo_path}")
    
    # TEMP dataset
    temp_path = f"{PATHS['TEMP']}{depo_filename}"
    fcyfd.write_parquet(temp_path)
    print(f"  TEMP dataset: {temp_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total records: {len(fcyfd):,}")
    print(f"Total columns: {len(fcyfd.columns)}")
    print(f"\nFirst 5 records:")
    print(fcyfd.head(5))
    
    print("\n" + "=" * 60)
    print("✓ EIBWFCFD Complete")

if __name__ == "__main__":
    main()
