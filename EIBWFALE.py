"""
EIBWFALE - FD/IFD Data Extraction from Binary Deposit File
Extracts FD4001 records, processes customer codes, calculates maturity dates,
and splits into conventional and Islamic FD datasets.
Includes format handling and sample prints.
"""

import struct
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'DEPOSIT': 'data/deposit/',
    'FD': 'data/fd/',
    'IFD': 'data/ifd/',
    'FORMATS': 'data/formats/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# PACKED DECIMAL HELPERS
# =============================================================================
def pd(data, offset, bytes, decimals=0):
    """Extract packed decimal value"""
    if offset + bytes > len(data):
        return 0
    val = int.from_bytes(data[offset:offset+bytes], 'big')
    # Simple packed decimal interpretation
    return val / (10 ** decimals)

def read_str(data, offset, length):
    """Read string"""
    if offset + length > len(data):
        return ''
    return data[offset:offset+length].decode('ascii', errors='ignore').strip()

# =============================================================================
# FORMAT LOADING (equivalent to %INC PGM(PBBDPFMT))
# =============================================================================
def load_formats():
    """Load formats like FDDENOM from format library"""
    formats = {}
    try:
        # Try to load FDDENOM format
        fmt_file = Path(PATHS['FORMATS']) / "FDDENOM.parquet"
        if fmt_file.exists():
            fmt_df = pl.read_parquet(fmt_file)
            for row in fmt_df.rows(named=True):
                formats[row['START']] = row['LABEL']
    except:
        # Default format if not found
        pass
    return formats

# =============================================================================
# REPORT DATE PROCESSING
# =============================================================================
def get_report_date():
    """Read first record from DEPOSIT file to get report date"""
    dep_file = Path(PATHS['DEPOSIT']) / "DEPOSIT.dat"
    
    if not dep_file.exists():
        today = datetime.now()
        return today, (today + timedelta(days=1)).strftime('%j')
    
    with open(dep_file, 'rb') as f:
        data = f.read(1752)
        if len(data) < 112:
            return datetime.now(), datetime.now().strftime('%j')
        
        # @106 TBDATE PD6.
        tbdate = int.from_bytes(data[105:111], 'big')
        tbdate_str = f"{tbdate:011d}"[:8]
        
        # Parse MMDDYYYY
        reptdate = datetime(int(tbdate_str[4:8]), int(tbdate_str[0:2]), int(tbdate_str[2:4]))
        reptdatx = reptdate + timedelta(days=1)
        xdate = reptdatx.strftime('%j')
        
        return reptdate, reptdatx, xdate

# =============================================================================
# CUSTCD CONVERSION
# =============================================================================
def convert_custcd(raw):
    """Convert CUSTCD based on value ranges"""
    if 0 < raw <= 99:
        return raw
    elif raw <= 999:
        return int(str(raw)[1:3])
    elif raw <= 9999:
        return int(str(raw)[2:4])
    elif raw <= 99999:
        return int(str(raw)[3:5])
    return 0

# =============================================================================
# MAIN EXTRACTION
# =============================================================================
def main():
    print("=" * 60)
    print("EIBWFALE - FD/IFD Data Extraction")
    print("=" * 60)
    
    # Load formats (equivalent to %INC PGM(PBBDPFMT))
    formats = load_formats()
    print(f"Formats loaded: {len(formats)}")
    
    # Get report date
    reptdate, reptdatx, xdate = get_report_date()
    print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Next Day: {reptdatx.strftime('%d/%m/%Y')}")
    print(f"XDATE (Julian): {xdate}")
    
    # Save REPTDATE datasets (both FD and IFD)
    rept_df = pl.DataFrame({'REPTDATE': [reptdate]})
    rept_df.write_parquet(f"{PATHS['FD']}REPTDATE.parquet")
    rept_df.write_parquet(f"{PATHS['IFD']}REPTDATE.parquet")
    print(f"\nREPTDATE saved to FD and IFD directories")
    
    # Extract FD4001 records
    dep_file = Path(PATHS['DEPOSIT']) / "DEPOSIT.dat"
    records = []
    
    if not dep_file.exists():
        print(f"Error: {dep_file} not found")
        return
    
    with open(dep_file, 'rb') as f:
        record_num = 0
        while True:
            data = f.read(1752)
            if len(data) < 1752:
                break
            
            record_num += 1
            
            # Check if FD4001 record
            bankno = pd(data, 2, 2)      # @3 BANKNO
            reptno = pd(data, 23, 3)     # @24 REPTNO
            fmtcode = pd(data, 26, 2)    # @27 FMTCODE
            
            if bankno == 33 and reptno == 4001 and fmtcode in (1, 2):
                custcd_raw = pd(data, 123, 3)
                intplan = pd(data, 207, 2)
                renewal = read_str(data, 210, 1)
                lmatdate = pd(data, 391, 6)
                costctr = pd(data, 408, 4)
                
                # Calculate maturity date if applicable
                matdate = pd(data, 178, 5)  # Default MATDATE
                if lmatdate > 0 and renewal == 'N':
                    lmat_str = f"{lmatdate:011d}"[:8]
                    try:
                        lmat_dt = datetime(int(lmat_str[4:8]), int(lmat_str[0:2]), int(lmat_str[2:4]))
                        if lmat_dt.strftime('%j') == xdate:
                            matdate = int(f"{lmat_dt.year:04d}{lmat_dt.month:02d}{lmat_dt.day:02d}")
                    except:
                        pass
                
                # AMTIND using FDDENOM format
                amtind = str(intplan)
                if intplan in formats:
                    amtind = formats[intplan]
                
                records.append({
                    'BRANCH': pd(data, 105, 4),
                    'ACCTNO': pd(data, 109, 6),
                    'STATEC': read_str(data, 115, 6),
                    'PURPOSE': read_str(data, 121, 1),
                    'CUSTCD': convert_custcd(custcd_raw),
                    'DEPODTE': pd(data, 126, 6),
                    'NAME': read_str(data, 133, 15),
                    'CDNO': pd(data, 148, 6),
                    'OPENIND': read_str(data, 154, 1),
                    'CURBAL': pd(data, 155, 6, 2),
                    'ORIGAMT': pd(data, 167, 6, 2),
                    'ORGDATE': pd(data, 173, 5),
                    'MATDATE': matdate,
                    'RATE': pd(data, 188, 3, 3),
                    'ACCTTYPE': pd(data, 202, 2),
                    'TERM': pd(data, 204, 2),
                    'MATID': read_str(data, 206, 1),
                    'INTPLAN': intplan,
                    'PAYMENT': read_str(data, 209, 1),
                    'RENEWAL': renewal,
                    'INTPDYTD': pd(data, 211, 6, 2),
                    'INTPAY': pd(data, 217, 6, 2),
                    'INTDATE': pd(data, 229, 5),
                    'LASTACTV': pd(data, 239, 5),
                    'INTFREQ': pd(data, 244, 2),
                    'INTFREQID': read_str(data, 246, 1),
                    'PENDINT': pd(data, 303, 2),
                    'CURCODE': read_str(data, 348, 3),
                    'LMATDATE': lmatdate,
                    'PRORATIO': pd(data, 404, 3),
                    'FDHOLD': read_str(data, 407, 1),
                    'COSTCTR': costctr,
                    'COLLNO': pd(data, 412, 6),
                    'INTTFRACCT': pd(data, 418, 6),
                    'PRN_DISP_OPT': read_str(data, 424, 1),
                    'PRN_RENEW': pd(data, 425, 6),
                    'PRN_TFR_ACCT': pd(data, 431, 6),
                    'AMTIND': amtind
                })
    
    print(f"\nExtracted: {len(records):,} FD4001 records")
    
    if not records:
        return
    
    # Create DataFrame and sort (PROC SORT)
    df = pl.DataFrame(records)
    df = df.sort(['ACCTNO', 'CDNO'])
    
    # Split into Conventional and Islamic
    fd_df = df.filter((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999))
    ifd_df = df.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 3999))
    
    # Save datasets
    fd_df.write_parquet(f"{PATHS['FD']}FD.parquet")
    ifd_df.write_parquet(f"{PATHS['IFD']}FD.parquet")
    
    print(f"\nConventional FD: {len(fd_df):,} records")
    print(f"Islamic FD: {len(ifd_df):,} records")
    
    # PROC PRINT statements (sample data)
    print("\n" + "=" * 60)
    print("PROC PRINT DATA = FD.FD (OBS=20)")
    print("=" * 60)
    print(fd_df.head(20))
    
    print("\n" + "=" * 60)
    print("PROC PRINT DATA = IFD.FD (OBS=20)")
    print("=" * 60)
    print(ifd_df.head(20))
    
    print("\n" + "=" * 60)
    print("PROC PRINT DATA = FD.REPTDATE")
    print("=" * 60)
    print(rept_df)
    
    print("\n" + "=" * 60)
    print("✓ EIBWFALE Complete")

if __name__ == "__main__":
    main()
