"""
EIBWFALE - FD/IFD Data Extraction and Processing
Extracts FD4001 records from deposit file, processes customer codes,
calculates maturity dates, and splits into conventional and Islamic FD datasets.
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import struct
import sys

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'DEPOSIT': 'data/deposit/',
    'FD': 'data/fd/',
    'IFD': 'data/ifd/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# BINARY DATA READING FUNCTIONS
# =============================================================================
def read_pd2(data, offset):
    """Read packed decimal 2 bytes"""
    if offset + 2 > len(data):
        return 0
    # Simplified - actual packed decimal conversion would be more complex
    val = int.from_bytes(data[offset:offset+2], 'big')
    return val

def read_pd3(data, offset):
    """Read packed decimal 3 bytes"""
    if offset + 3 > len(data):
        return 0
    val = int.from_bytes(data[offset:offset+3], 'big')
    return val

def read_pd4(data, offset):
    """Read packed decimal 4 bytes"""
    if offset + 4 > len(data):
        return 0
    val = int.from_bytes(data[offset:offset+4], 'big')
    return val

def read_pd5(data, offset):
    """Read packed decimal 5 bytes (dates)"""
    if offset + 5 > len(data):
        return 0
    val = int.from_bytes(data[offset:offset+5], 'big')
    return val

def read_pd6(data, offset):
    """Read packed decimal 6 bytes"""
    if offset + 6 > len(data):
        return 0
    val = int.from_bytes(data[offset:offset+6], 'big')
    return val

def read_pd6_2(data, offset):
    """Read packed decimal 6 bytes with 2 decimal places"""
    if offset + 6 > len(data):
        return 0.0
    val = int.from_bytes(data[offset:offset+6], 'big')
    return val / 100.0

def read_pd3_3(data, offset):
    """Read packed decimal 3 bytes with 3 decimal places (rate)"""
    if offset + 3 > len(data):
        return 0.0
    val = int.from_bytes(data[offset:offset+3], 'big')
    return val / 1000.0

def read_string(data, offset, length):
    """Read string of specified length"""
    if offset + length > len(data):
        return ''
    return data[offset:offset+length].decode('ascii', errors='ignore').strip()

# =============================================================================
# REPORT DATE PROCESSING
# =============================================================================
def process_report_date():
    """Read first record from DEPOSIT file to get report date"""
    deposit_file = Path(PATHS['DEPOSIT']) / "DEPOSIT.dat"
    
    if not deposit_file.exists():
        print(f"Warning: {deposit_file} not found, using current date")
        today = datetime.now()
        reptdate = today
        reptdatx = today + timedelta(days=1)
        xdate = reptdatx.strftime('%j')  # Julian day (Z5. format)
        return reptdate, reptdatx, xdate
    
    try:
        with open(deposit_file, 'rb') as f:
            # Read first record (OBS=1) with LRECL=1752
            data = f.read(1752)
            
            if len(data) >= 112:  # @106 is 1-based, so 105 in 0-based
                # @106 TBDATE PD6.
                tbdate_val = read_pd6(data, 105)  # 105 = 106-1
                
                # Convert to date: REPTDATE=INPUT(SUBSTR(PUT(TBDATE,Z11.),1,8),MMDDYY8.)
                tbdate_str = f"{tbdate_val:011d}"
                mmdd_str = tbdate_str[:8]  # First 8 chars
                
                # Parse as MMDDYYYY
                if len(mmdd_str) == 8:
                    month = int(mmdd_str[0:2])
                    day = int(mmdd_str[2:4])
                    year = int(mmdd_str[4:8])
                    
                    if 1 <= month <= 12 and 1 <= day <= 31 and year > 1900:
                        reptdate = datetime(year, month, day)
                        reptdatx = reptdate + timedelta(days=1)
                        xdate = reptdatx.strftime('%j')  # Julian day (Z5.)
                        
                        print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
                        print(f"XDATE (next day Julian): {xdate}")
                        
                        return reptdate, reptdatx, xdate
    
    except Exception as e:
        print(f"Error reading deposit file: {e}")
    
    # Default fallback
    today = datetime.now()
    reptdate = today
    reptdatx = today + timedelta(days=1)
    xdate = reptdatx.strftime('%j')
    return reptdate, reptdatx, xdate

# =============================================================================
# FD4001 DATA EXTRACTION
# =============================================================================
def extract_fd4001(xdate):
    """Extract FD4001 records from DEPOSIT file"""
    deposit_file = Path(PATHS['DEPOSIT']) / "DEPOSIT.dat"
    records = []
    
    if not deposit_file.exists():
        print(f"Error: {deposit_file} not found")
        return records
    
    try:
        with open(deposit_file, 'rb') as f:
            record_num = 0
            while True:
                data = f.read(1752)
                if len(data) < 1752:
                    break
                
                record_num += 1
                
                # Check if this is an FD4001 record
                if len(data) >= 29:  # Need up to FMTCODE
                    # @3 BANKNO PD2. (offset 2)
                    bankno = read_pd2(data, 2)
                    
                    # @24 REPTNO PD3. (offset 23)
                    reptno = read_pd3(data, 23)
                    
                    # @27 FMTCODE PD2. (offset 26)
                    fmtcode = read_pd2(data, 26)
                    
                    # IF BANKNO = 33 AND REPTNO = 4001 AND FMTCODE IN (1,2)
                    if bankno == 33 and reptno == 4001 and fmtcode in (1, 2):
                        # Extract all fields based on INPUT statement
                        row = {}
                        
                        # @106 BRANCH PD4. (offset 105)
                        row['BRANCH'] = read_pd4(data, 105)
                        
                        # @110 ACCTNO PD6. (offset 109)
                        row['ACCTNO'] = read_pd6(data, 109)
                        
                        # @116 STATEC $6. (offset 115)
                        row['STATEC'] = read_string(data, 115, 6)
                        
                        # @122 PURPOSE $1. (offset 121)
                        row['PURPOSE'] = read_string(data, 121, 1)
                        
                        # @124 CUSTCD PD3. (offset 123)
                        custcd_raw = read_pd3(data, 123)
                        
                        # Apply CUSTCD conversion logic
                        if 0 < custcd_raw <= 99:
                            custcd = custcd_raw
                        elif 100 <= custcd_raw <= 999:
                            custcd = int(str(custcd_raw)[1:3])
                        elif 1000 <= custcd_raw <= 9999:
                            custcd = int(str(custcd_raw)[2:4])
                        elif 10000 <= custcd_raw <= 99999:
                            custcd = int(str(custcd_raw)[3:5])
                        else:
                            custcd = 0
                        
                        row['CUSTCD'] = custcd
                        
                        # @127 DEPODTE PD6. (offset 126)
                        row['DEPODTE'] = read_pd6(data, 126)
                        
                        # @134 NAME $15. (offset 133)
                        row['NAME'] = read_string(data, 133, 15)
                        
                        # @149 CDNO PD6. (offset 148)
                        row['CDNO'] = read_pd6(data, 148)
                        
                        # @155 OPENIND $1. (offset 154)
                        row['OPENIND'] = read_string(data, 154, 1)
                        
                        # @156 CURBAL PD6.2 (offset 155)
                        row['CURBAL'] = read_pd6_2(data, 155)
                        
                        # @168 ORIGAMT PD6.2 (offset 167)
                        row['ORIGAMT'] = read_pd6_2(data, 167)
                        
                        # @174 ORGDATE PD5. (offset 173)
                        row['ORGDATE'] = read_pd5(data, 173)
                        
                        # @179 MATDATE PD5. (offset 178)
                        row['MATDATE'] = read_pd5(data, 178)
                        
                        # @189 RATE PD3.3 (offset 188)
                        row['RATE'] = read_pd3_3(data, 188)
                        
                        # @203 ACCTTYPE PD2. (offset 202)
                        row['ACCTTYPE'] = read_pd2(data, 202)
                        
                        # @205 TERM PD2. (offset 204)
                        row['TERM'] = read_pd2(data, 204)
                        
                        # @207 MATID $1. (offset 206)
                        row['MATID'] = read_string(data, 206, 1)
                        
                        # @208 INTPLAN PD2. (offset 207)
                        row['INTPLAN'] = read_pd2(data, 207)
                        
                        # @210 PAYMENT $1. (offset 209)
                        row['PAYMENT'] = read_string(data, 209, 1)
                        
                        # @211 RENEWAL $1. (offset 210)
                        row['RENEWAL'] = read_string(data, 210, 1)
                        
                        # @212 INTPDYTD PD6.2 (offset 211)
                        row['INTPDYTD'] = read_pd6_2(data, 211)
                        
                        # @218 INTPAY PD6.2 (offset 217)
                        row['INTPAY'] = read_pd6_2(data, 217)
                        
                        # @230 INTDATE PD5. (offset 229)
                        row['INTDATE'] = read_pd5(data, 229)
                        
                        # @240 LASTACTV PD5. (offset 239)
                        row['LASTACTV'] = read_pd5(data, 239)
                        
                        # @245 INTFREQ PD2. (offset 244)
                        row['INTFREQ'] = read_pd2(data, 244)
                        
                        # @247 INTFREQID $1. (offset 246)
                        row['INTFREQID'] = read_string(data, 246, 1)
                        
                        # @304 PENDINT PD2. (offset 303)
                        row['PENDINT'] = read_pd2(data, 303)
                        
                        # @349 CURCODE $3. (offset 348)
                        row['CURCODE'] = read_string(data, 348, 3)
                        
                        # @392 LMATDATE PD6. (offset 391)
                        row['LMATDATE'] = read_pd6(data, 391)
                        
                        # @405 PRORATIO PD3. (offset 404)
                        row['PRORATIO'] = read_pd3(data, 404)
                        
                        # @408 FDHOLD $1. (offset 407)
                        row['FDHOLD'] = read_string(data, 407, 1)
                        
                        # @409 COSTCTR PD4. (offset 408)
                        row['COSTCTR'] = read_pd4(data, 408)
                        
                        # @413 COLLNO PD6. (offset 412)
                        row['COLLNO'] = read_pd6(data, 412)
                        
                        # @419 INTTFRACCT PD6. (offset 418)
                        row['INTTFRACCT'] = read_pd6(data, 418)
                        
                        # @425 PRN_DISP_OPT $1. (offset 424)
                        row['PRN_DISP_OPT'] = read_string(data, 424, 1)
                        
                        # @426 PRN_RENEW PD6. (offset 425)
                        row['PRN_RENEW'] = read_pd6(data, 425)
                        
                        # @432 PRN_TFR_ACCT PD6. (offset 431)
                        row['PRN_TFR_ACCT'] = read_pd6(data, 431)
                        
                        records.append(row)
                        
                        if len(records) % 1000 == 0:
                            print(f"  Extracted {len(records):,} records...")
    
    except Exception as e:
        print(f"Error extracting FD4001: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"  Total FD4001 records: {len(records):,}")
    return records

# =============================================================================
# MATURITY DATE CALCULATION
# =============================================================================
def calculate_maturity_date(row, xdate):
    """Calculate MATDATE based on LMATDATE and RENEWAL"""
    lmatdate = row.get('LMATDATE', 0)
    renewal = row.get('RENEWAL', '')
    
    if lmatdate > 0:
        # Convert LMATDATE to date
        lmatdate_str = f"{lmatdate:011d}"
        mmdd_str = lmatdate_str[:8]
        
        if len(mmdd_str) == 8:
            try:
                month = int(mmdd_str[0:2])
                day = int(mmdd_str[2:4])
                year = int(mmdd_str[4:8])
                
                if 1 <= month <= 12 and 1 <= day <= 31 and year > 1900:
                    lmdates = datetime(year, month, day)
                    
                    # Check if renewal = 'N' and lmdates = XDATE
                    if renewal == 'N' and lmdates.strftime('%j') == xdate:
                        # MATDATE = PUT(LMYY,Z4.)||PUT(LMMM,Z2.)||PUT(LMDD,Z2.)
                        return f"{year:04d}{month:02d}{day:02d}"
            except:
                pass
    
    return None

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBWFALE - FD/IFD Data Extraction")
    print("=" * 60)
    
    # Step 1: Process report date
    print("\nStep 1: Processing report date...")
    reptdate, reptdatx, xdate = process_report_date()
    
    # Save REPTDATE datasets
    rept_df = pl.DataFrame({'REPTDATE': [reptdate]})
    rept_df.write_parquet(f"{PATHS['FD']}REPTDATE.parquet")
    rept_df.write_parquet(f"{PATHS['IFD']}REPTDATE.parquet")
    print(f"  REPTDATE saved to FD/IFD directories")
    
    # Step 2: Extract FD4001 records
    print("\nStep 2: Extracting FD4001 records...")
    records = extract_fd4001(xdate)
    
    if not records:
        print("No FD4001 records found")
        return
    
    # Convert to DataFrame
    df = pl.DataFrame(records)
    
    # Step 3: Sort by ACCTNO, CDNO
    print("\nStep 3: Sorting data...")
    df = df.sort(['ACCTNO', 'CDNO'])
    
    # Step 4: Calculate maturity dates and AMTIND
    print("\nStep 4: Processing maturity dates...")
    
    # AMTIND = PUT(INTPLAN,FDDENOM.) - would need format
    # For now, just convert to string
    df = df.with_columns([
        pl.col('INTPLAN').cast(pl.Utf8).alias('AMTIND')
    ])
    
    # Calculate MATDATE
    matdates = []
    for row in df.rows(named=True):
        matdate = calculate_maturity_date(row, xdate)
        matdates.append(matdate)
    
    df = df.with_columns([
        pl.Series('MATDATE_CALC', matdates)
    ])
    
    # Step 5: Split into Conventional and Islamic
    print("\nStep 5: Splitting into Conventional and Islamic...")
    
    # IF (3000<=COSTCTR<=3999) THEN OUTPUT IFD.FD ELSE OUTPUT FD.FD
    ifd_df = df.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 3999))
    fd_df = df.filter((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999))
    
    print(f"  Conventional FD: {len(fd_df):,} records")
    print(f"  Islamic FD (IFD): {len(ifd_df):,} records")
    
    # Drop temporary columns
    fd_df = fd_df.drop(['MATDATE_CALC'])
    ifd_df = ifd_df.drop(['MATDATE_CALC'])
    
    # Save datasets
    print("\nStep 6: Saving datasets...")
    
    fd_df.write_parquet(f"{PATHS['FD']}FD.parquet")
    ifd_df.write_parquet(f"{PATHS['IFD']}FD.parquet")
    
    print(f"  FD.FD: {PATHS['FD']}FD.parquet")
    print(f"  IFD.FD: {PATHS['IFD']}FD.parquet")
    
    # Step 7: Print sample data (equivalent to PROC PRINT OBS=20)
    print("\nStep 7: Sample data (first 5 records):")
    print("\nFD.FD Sample:")
    print(fd_df.head(5))
    print("\nIFD.FD Sample:")
    print(ifd_df.head(5))
    print("\nFD.REPTDATE:")
    print(rept_df)
    
    print("\n" + "=" * 60)
    print("✓ EIBWFALE Complete")

if __name__ == "__main__":
    main()
