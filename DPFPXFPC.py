import polars as pl
from pathlib import Path
import datetime
import struct
import codecs

# Configuration
deposit_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQPIUC")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQPIUC")
input_file = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQPIUC/MAREMUC5") 

output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)

# DATA REPTDATE (KEEP=REPTDATE);
# REPTDATE=INPUT('01'||PUT(MONTH(TODAY()), Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1;
today = datetime.date.today()
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# SELECT(DAY(REPTDATE)); logic
reptday = reptdate.day
if reptday == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif reptday == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif reptday == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1, WK2, WK3 = 23, '4', '3', '2', '1'

MM = reptdate.month

# IF WK = '1' THEN DO;
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

# MM2 = MM - 1;
MM2 = MM - 1
if MM2 == 0:
    MM2 = 12

SDATE = datetime.date(reptdate.year, MM, SDD)
SDESC = 'PUBLIC BANK BERHAD'

# CALL SYMPUT equivalents
NOWK = WK
REPTMON = f"{MM:02d}"
REPTYEAR = str(reptdate.year)

print(f"NOWK: {NOWK}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
print(f"SDESC: {SDESC}")
print(f"REPTDATE: {reptdate}")
print(f"SDATE: {SDATE}")

# Create REPTDATE DataFrame (KEEP=REPTDATE)
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

def unpack_packed_decimal(data):
    """
    Unpack a packed decimal (PD) field.
    SAS PD format stores numbers in BCD (Binary Coded Decimal) format.
    Each byte contains two digits, with the last nibble being the sign.
    """
    if not data:
        return None
    
    # Convert bytes to hex string
    hex_str = data.hex()
    digits = []
    sign = 1
    
    # Process each nibble (half-byte)
    for i, char in enumerate(hex_str):
        if char.isdigit():
            digits.append(char)
        elif char in 'abcdef':
            # This is the sign nibble
            # Last nibble contains sign: C=positive, D=negative, F=unsigned
            if char in 'cC':
                sign = 1
            elif char in 'dD':
                sign = -1
            # F means unsigned (positive)
            elif char in 'fF':
                sign = 1
    
    # If we have digits, convert to number
    if digits:
        # Remove leading zeros
        while len(digits) > 1 and digits[0] == '0':
            digits.pop(0)
        
        # Build the number
        num_str = ''.join(digits)
        return sign * int(num_str) if num_str else None
    
    return None

def unpack_packed_decimal_with_decimal(data, decimal_places=2):
    """
    Unpack a packed decimal with decimal places (PD7.2 format)
    """
    if not data:
        return None
    
    hex_str = data.hex()
    digits = []
    sign = 1
    
    for i, char in enumerate(hex_str):
        if char.isdigit():
            digits.append(char)
        elif char in 'abcdef':
            if char in 'cC':
                sign = 1
            elif char in 'dD':
                sign = -1
            elif char in 'fF':
                sign = 1
    
    if digits:
        # Remove leading zeros but keep at least one digit
        while len(digits) > 1 and digits[0] == '0':
            digits.pop(0)
        
        num_str = ''.join(digits)
        if num_str:
            # Insert decimal point
            if len(num_str) > decimal_places:
                num_str = num_str[:-decimal_places] + '.' + num_str[-decimal_places:]
            else:
                num_str = '0.' + num_str.zfill(decimal_places)
            
            return sign * float(num_str)
    
    return None

def parse_record_binary(record):
    """
    Parse a record using binary mode for packed decimal fields
    """
    try:
        # Ensure record is long enough
        if len(record) < 114:
            return None
        
        # Extract fields as binary
        acctno_data = record[2:8]      # @003 PD6.
        ledgbal_data = record[39:46]   # @040 PD7.2
        status = record[46:47]         # @047 $1.
        paymode = record[54:64]        # @055 $10.
        name = record[74:114]          # @075 $40.
        
        # Unpack packed decimals
        acctno = unpack_packed_decimal(acctno_data)
        ledgbal = unpack_packed_decimal_with_decimal(ledgbal_data, 2)
        
        # Decode character fields (try ASCII first, fallback to EBCDIC)
        status_str = status.decode('ascii', errors='ignore').strip()
        paymode_str = paymode.decode('ascii', errors='ignore').strip()
        name_str = name.decode('ascii', errors='ignore').strip()
        
        # If ASCII decoding gave empty strings, try EBCDIC
        if not status_str and status:
            try:
                status_str = status.decode('cp500', errors='ignore').strip()
            except:
                pass
        if not paymode_str and paymode:
            try:
                paymode_str = paymode.decode('cp500', errors='ignore').strip()
            except:
                pass
        if not name_str and name:
            try:
                name_str = name.decode('cp500', errors='ignore').strip()
            except:
                pass
        
        return {
            'ACCTNO': acctno if acctno is not None else 0,
            'LEDGBAL': ledgbal if ledgbal is not None else 0.0,
            'STATUS': status_str,
            'PAYMODE': paymode_str,
            'NAME': name_str
        }
        
    except Exception as e:
        return None

def read_binary_flat_file(filepath):
    """
    Read the binary flat file with packed decimal fields
    """
    records = []
    
    try:
        with open(filepath, 'rb') as f:
            data = f.read()
            
            print(f"File size: {len(data)} bytes")
            
            # Try to find records by looking for 'rW' pattern
            positions = []
            start_pos = 0
            while True:
                pos = data.find(b'rW', start_pos)
                if pos == -1:
                    break
                positions.append(pos)
                start_pos = pos + 1
            
            print(f"Found {len(positions)} potential records")
            
            if positions:
                # Use the positions to extract records
                for i, pos in enumerate(positions):
                    # Determine record length (up to next marker or end of file)
                    if i < len(positions) - 1:
                        end_pos = positions[i + 1]
                    else:
                        end_pos = len(data)
                    
                    # Extract the record (from 'rW' position)
                    record = data[pos:end_pos]
                    
                    # Try to parse this record
                    parsed = parse_record_binary(record)
                    if parsed and parsed['ACCTNO'] != 0:  # Valid account number
                        records.append(parsed)
            else:
                # Try fixed length parsing (115 bytes)
                record_length = 115
                for i in range(0, len(data) - record_length, record_length):
                    record = data[i:i+record_length]
                    parsed = parse_record_binary(record)
                    if parsed and parsed['ACCTNO'] != 0:
                        records.append(parsed)
            
            print(f"Successfully parsed {len(records)} records")
            
    except Exception as e:
        print(f"Error reading file: {e}")
        import traceback
        traceback.print_exc()
    
    return records

# Main processing
print(f"\nReading binary flat file: {input_file}")
records = read_binary_flat_file(input_file)

if records:
    # Convert to Polars DataFrame
    df = pl.DataFrame(records)
    
    print(f"\nProcessed {len(df)} records")
    print("\nSample of parsed data (first 5 records):")
    print(df.head(5))
    
    print("\nData types:")
    print(df.dtypes)
    
    print("\nNull counts:")
    print(df.null_count())
    
    # Clean up - remove records with missing data
    # Use the correct Polars syntax for combining conditions
    df = df.filter(
        pl.col('ACCTNO').is_not_null() & 
        pl.col('LEDGBAL').is_not_null() & 
        pl.col('STATUS').is_not_null() & 
        pl.col('PAYMODE').is_not_null() & 
        pl.col('NAME').is_not_null()
    )
    
    print(f"\nAfter removing nulls: {len(df)} records")
    
    # Now apply the same logic as before
    # IF STATUS = 'U';
    if 'STATUS' in df.columns:
        unclaim_filtered = df.filter(pl.col('STATUS') == 'U')
        print(f"\nRecords with STATUS='U': {len(unclaim_filtered)}")
        
        # If we have STATUS='U' records, process them
        if len(unclaim_filtered) > 0:
            # Create CATEGORY based on PAYMODE
            unclaim_with_category = unclaim_filtered.with_columns([
                pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '6']))
                .then(pl.lit('SA'))
                .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
                .then(pl.lit('CA'))
                .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
                .then(pl.lit('FD'))
                .otherwise(pl.lit('OTHER'))
                .alias('CATEGORY')
            ])
            
            # Split into UNCLAIM and NOTUNCLAIM based on PAYMODE
            valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
            
            unclaim_valid = unclaim_with_category.filter(
                pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
            )
            
            notunclaim_invalid = unclaim_with_category.filter(
                ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
            )
            
            print(f"\nUNCLAIM valid records (PAYMODE 1-9): {len(unclaim_valid)}")
            print(f"NOTUNCLAIM invalid records: {len(notunclaim_invalid)}")
            
            # Save the data - use 2025 since that's the data year
            # The data is from 2024-2025 based on the sample
            data_year = "2025"  # You can change this as needed
            
            if not unclaim_valid.is_empty():
                unclaim_sorted = unclaim_valid.sort('PAYMODE')
                unclaim_sorted.write_parquet(deposit_path / f"UNCLAIM{data_year}.parquet")
                unclaim_sorted.write_csv(deposit_path / f"UNCLAIM{data_year}.csv")
                print(f"\nSaved UNCLAIM data with {len(unclaim_sorted)} records")
                
                # Summary by PAYMODE
                unclaim_summary = unclaim_sorted.group_by('PAYMODE').agg([
                    pl.col('LEDGBAL').sum().alias('LEDGBAL')
                ])
                
                print("\nUNCLAIM SUMMARY BY PAYMODE:")
                print(unclaim_summary)
                
                total_ledgbal = unclaim_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
                print(f"\nUNCLAIM TOTAL LEDGBAL: {total_ledgbal:,.2f}")
                
                # Save summary
                unclaim_summary.write_csv(output_path / f"UNCLAIM_Summary_{data_year}.csv")
                
                # Summary by CATEGORY
                category_summary = unclaim_sorted.group_by('CATEGORY').agg([
                    pl.count().alias('COUNT'),
                    pl.col('LEDGBAL').sum().alias('TOTAL_AMOUNT')
                ])
                print("\nSUMMARY BY CATEGORY:")
                print(category_summary)
                category_summary.write_csv(output_path / f"Category_Summary_{data_year}.csv")
            
            if not notunclaim_invalid.is_empty():
                notunclaim_sorted = notunclaim_invalid.sort(['PAYMODE', 'NAME'])
                notunclaim_sorted.write_parquet(deposit_path / f"NOTUNCLAIM{data_year}.parquet")
                notunclaim_sorted.write_csv(deposit_path / f"NOTUNCLAIM{data_year}.csv")
                print(f"Saved NOTUNCLAIM data with {len(notunclaim_sorted)} records")
                
                if not notunclaim_sorted.is_empty():
                    total_ledgbal = notunclaim_sorted.select(pl.col('LEDGBAL').sum()).row(0)[0]
                    print(f"\nNOTUNCLAIM TOTAL LEDGBAL: {total_ledgbal:,.2f}")
        else:
            print("No records with STATUS='U' found")
            # Process all records as UNCLAIM if no STATUS='U'
            print("Processing all records as UNCLAIM...")
            
            unclaim_with_category = df.with_columns([
                pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '6']))
                .then(pl.lit('SA'))
                .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
                .then(pl.lit('CA'))
                .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
                .then(pl.lit('FD'))
                .otherwise(pl.lit('OTHER'))
                .alias('CATEGORY')
            ])
            
            valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
            unclaim_valid = unclaim_with_category.filter(
                pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
            )
            
            if not unclaim_valid.is_empty():
                data_year = "2025"
                unclaim_sorted = unclaim_valid.sort('PAYMODE')
                unclaim_sorted.write_parquet(deposit_path / f"UNCLAIM{data_year}.parquet")
                unclaim_sorted.write_csv(deposit_path / f"UNCLAIM{data_year}.csv")
                print(f"Saved {len(unclaim_sorted)} records as UNCLAIM")
    else:
        print("No STATUS column found in data")
    
else:
    print("No valid records were parsed from the file")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
