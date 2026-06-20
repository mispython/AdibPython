import polars as pl
from pathlib import Path
import datetime
import struct
import codecs

# Configuration
deposit_path = Path("DEPOSIT")
output_path = Path("output")
input_file = Path("MAREMUCC5")

output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)

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
            # If we encounter a non-digit before the last byte, it might be part of the number
            elif i < len(hex_str) - 1:
                # This could be a valid hex digit in a number
                # But for packed decimal, we only want digits
                pass
    
    # If we have digits, convert to number
    if digits:
        # Remove leading zeros
        while len(digits) > 1 and digits[0] == '0':
            digits.pop(0)
        
        # Build the number
        num_str = ''.join(digits)
        # Handle decimal point if needed (we'll handle this separately for LEDGBAL)
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

def parse_record_ebcdic(record):
    """
    Parse a record using EBCDIC encoding for character fields.
    The file appears to be in EBCDIC (mainframe) format.
    """
    try:
        # Check if we have EBCDIC data (has non-ASCII control chars)
        # Convert to EBCDIC first if needed
        try:
            # Try to decode as EBCDIC
            ebcdic_decoded = record.decode('cp500')  # IBM EBCDIC
        except:
            # If EBCDIC decode fails, treat as binary
            ebcdic_decoded = record.decode('latin-1', errors='ignore')
        
        # Now parse based on positions (0-indexed Python positions)
        # @003 -> index 2
        # ACCTNO: PD6. (6 bytes packed decimal)
        acctno_data = record[2:8]  # positions 3-8 (1-indexed)
        
        # @040 -> index 39
        # LEDGBAL: PD7.2 (7 bytes packed decimal with 2 decimal places)
        ledgbal_data = record[39:46]  # positions 40-46 (1-indexed)
        
        # @047 -> index 46
        # STATUS: $1. (1 byte character)
        status = record[46:47].decode('ascii', errors='ignore').strip()
        
        # @055 -> index 54
        # PAYMODE: $10. (10 bytes character)
        paymode = record[54:64].decode('ascii', errors='ignore').strip()
        
        # @075 -> index 74
        # NAME: $40. (40 bytes character)
        name = record[74:114].decode('ascii', errors='ignore').strip()
        
        # Unpack packed decimals
        acctno = unpack_packed_decimal(acctno_data)
        ledgbal = unpack_packed_decimal_with_decimal(ledgbal_data, 2)
        
        return {
            'ACCTNO': acctno,
            'LEDGBAL': ledgbal,
            'STATUS': status,
            'PAYMODE': paymode,
            'NAME': name
        }
        
    except Exception as e:
        print(f"Error parsing record: {e}")
        return None

def parse_record_binary(record):
    """
    Parse a record using binary mode for packed decimal fields
    """
    try:
        # Extract fields as binary
        acctno_data = record[2:8]
        ledgbal_data = record[39:46]
        status = record[46:47]
        paymode = record[54:64]
        name = record[74:114]
        
        # Try to unpack packed decimal (using different approaches)
        acctno = None
        ledgbal = None
        
        # Attempt to unpack using Python's struct for the packed decimal
        try:
            # First try as compressed decimal
            acctno_hex = acctno_data.hex()
            if acctno_hex:
                # Convert packed decimal to number
                digits = []
                for i in range(0, len(acctno_hex), 2):
                    byte = acctno_hex[i:i+2]
                    if len(byte) == 2:
                        # Each byte contains two digits
                        d1 = int(byte[0], 16) if byte[0].isdigit() else 0
                        d2 = int(byte[1], 16) if byte[1].isdigit() else 0
                        digits.extend([str(d1), str(d2)])
                # Remove trailing sign nibble if present
                if digits and digits[-1] in 'cdef':
                    sign_char = digits.pop()
                num_str = ''.join(digits)
                acctno = int(num_str) if num_str else None
        except:
            pass
        
        # Try to unpack LEDGBAL similarly
        try:
            ledgbal_hex = ledgbal_data.hex()
            if ledgbal_hex:
                digits = []
                for i in range(0, len(ledgbal_hex), 2):
                    byte = ledgbal_hex[i:i+2]
                    if len(byte) == 2:
                        d1 = int(byte[0], 16) if byte[0].isdigit() else 0
                        d2 = int(byte[1], 16) if byte[1].isdigit() else 0
                        digits.extend([str(d1), str(d2)])
                if digits and digits[-1] in 'cdef':
                    digits.pop()
                num_str = ''.join(digits)
                if num_str:
                    # Insert decimal point 2 places from right
                    if len(num_str) > 2:
                        ledgbal = float(num_str[:-2] + '.' + num_str[-2:])
                    else:
                        ledgbal = float('0.' + num_str.zfill(2))
        except:
            pass
        
        return {
            'ACCTNO': acctno,
            'LEDGBAL': ledgbal,
            'STATUS': status.decode('ascii', errors='ignore').strip(),
            'PAYMODE': paymode.decode('ascii', errors='ignore').strip(),
            'NAME': name.decode('ascii', errors='ignore').strip()
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
            # Read the entire file
            data = f.read()
            
            # Look for record markers or fixed length records
            # Based on the sample, records appear to be separated by carriage returns
            # But they may be variable length due to packed decimal fields
            
            # The SAS INFILE with @ positions suggests fixed format
            # We'll try to parse assuming fixed length records of about 115 bytes
            # (based on the @075 NAME $40. plus previous fields)
            
            # Find record boundaries - look for patterns like 'rW' which appears to be a record marker
            # or use fixed length records
            record_start = 0
            record_marker = b'\x03\x14\x03rW'  # Pattern seen in the data
            
            # Try to find records by looking for 'rW' pattern
            positions = []
            start_pos = 0
            while True:
                pos = data.find(b'rW', start_pos)
                if pos == -1:
                    break
                positions.append(pos)
                start_pos = pos + 1
            
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
                    if parsed and parsed['ACCTNO'] is not None:
                        records.append(parsed)
                    else:
                        # Try EBCDIC parsing as fallback
                        parsed_ebcdic = parse_record_ebcdic(record)
                        if parsed_ebcdic and parsed_ebcdic['ACCTNO'] is not None:
                            records.append(parsed_ebcdic)
            else:
                # Try fixed length parsing (115 bytes)
                record_length = 115
                for i in range(0, len(data) - record_length, record_length):
                    record = data[i:i+record_length]
                    parsed = parse_record_binary(record)
                    if parsed and parsed['ACCTNO'] is not None:
                        records.append(parsed)
            
            print(f"Successfully parsed {len(records)} records")
            
    except Exception as e:
        print(f"Error reading file: {e}")
    
    return records

# Main processing
print(f"\nReading binary flat file: {input_file}")
records = read_binary_flat_file(input_file)

if records:
    # Convert to Polars DataFrame
    df = pl.DataFrame(records)
    
    # Clean up - remove records with missing data
    df = df.filter(pl.all().is_not_null())
    
    print(f"\nProcessed {len(df)} valid records")
    print("\nSample of parsed data:")
    print(df.head(10))
    
    # Now apply the same logic as before
    # IF STATUS = 'U';
    if 'STATUS' in df.columns:
        unclaim_filtered = df.filter(pl.col('STATUS') == 'U')
        print(f"\nRecords with STATUS='U': {len(unclaim_filtered)}")
    else:
        # If no STATUS column, create a default
        unclaim_filtered = df.with_columns(pl.lit('U').alias('STATUS'))
    
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
    
    # Save the data
    if not unclaim_valid.is_empty():
        unclaim_sorted = unclaim_valid.sort('PAYMODE')
        unclaim_sorted.write_parquet(deposit_path / f"UNCLAIM{REPTYEAR}.parquet")
        unclaim_sorted.write_csv(deposit_path / f"UNCLAIM{REPTYEAR}.csv")
        print(f"\nSaved UNCLAIM data with {len(unclaim_sorted)} records")
    
    if not notunclaim_invalid.is_empty():
        notunclaim_sorted = notunclaim_invalid.sort(['PAYMODE', 'NAME'])
        notunclaim_sorted.write_parquet(deposit_path / f"NOTUNCLAIM{REPTYEAR}.parquet")
        notunclaim_sorted.write_csv(deposit_path / f"NOTUNCLAIM{REPTYEAR}.csv")
        print(f"Saved NOTUNCLAIM data with {len(notunclaim_sorted)} records")
        
        # Summary
        if not notunclaim_sorted.is_empty():
            total_ledgbal = notunclaim_sorted.select(pl.col('LEDGBAL').sum()).row(0)[0]
            print(f"\nNOTUNCLAIM TOTAL LEDGBAL: {total_ledgbal:,.2f}")
    
    # Create summary by PAYMODE for UNCLAIM
    if not unclaim_valid.is_empty():
        unclaim_summary = unclaim_sorted.group_by('PAYMODE').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ])
        
        print("\nUNCLAIM SUMMARY BY PAYMODE:")
        print(unclaim_summary)
        
        total_ledgbal_final = unclaim_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nUNCLAIM TOTAL LEDGBAL: {total_ledgbal_final:,.2f}")
        
        # Save summary
        unclaim_summary.write_csv(output_path / f"UNCLAIM_Summary_{REPTYEAR}.csv")
        
        # Also save by CATEGORY
        category_summary = unclaim_sorted.group_by('CATEGORY').agg([
            pl.count().alias('COUNT'),
            pl.col('LEDGBAL').sum().alias('TOTAL_AMOUNT')
        ])
        print("\nSUMMARY BY CATEGORY:")
        print(category_summary)
        category_summary.write_csv(output_path / f"Category_Summary_{REPTYEAR}.csv")
    
else:
    print("No valid records were parsed from the file")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
