# =========================
# COLL file processing (EBCDIC with packed decimal)
# =========================
print("Processing COLL and DESC files...")

def read_ebcdic_fixed_records(filepath: Path, record_length: int, col_specs: list) -> pl.DataFrame:
    """
    Read EBCDIC file with fixed-length records
    For packed decimal (PD), we need to read the raw bytes and decode them properly
    """
    rows = []
    
    with open(filepath, 'rb') as f:
        while True:
            # Read one fixed-length record
            record = f.read(record_length)
            if not record or len(record) < record_length:
                break
                
            row = {}
            for col_name, start, end, col_type in col_specs:
                # SAS uses 1-based positions, convert to 0-based
                start_idx = start - 1
                end_idx = end
                
                if col_type == 'pd':
                    # Packed decimal - read raw bytes
                    raw_bytes = record[start_idx:end_idx]
                    
                    # Decode packed decimal
                    try:
                        # Convert bytes to hex string
                        hex_str = raw_bytes.hex()
                        
                        # Remove the sign nibble (last character)
                        digits = hex_str[:-1]
                        
                        # Check sign nibble
                        sign_nibble = hex_str[-1].upper()
                        
                        # Parse the digits
                        if digits and all(c in '0123456789ABCDEF' for c in digits):
                            value = int(digits, 16)
                            # Apply sign
                            if sign_nibble in ('D', 'B'):  # Negative in EBCDIC
                                value = -value
                            row[col_name.lower()] = float(value)
                        else:
                            row[col_name.lower()] = None
                    except:
                        row[col_name.lower()] = None
                        
                elif col_type == 'numeric':
                    # Regular numeric field - decode as EBCDIC then convert to float
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        # Remove any non-numeric characters
                        decoded_clean = ''.join(c for c in decoded if c.isdigit() or c in '.-')
                        row[col_name.lower()] = float(decoded_clean) if decoded_clean else None
                    except:
                        row[col_name.lower()] = None
                        
                else:  # character
                    # Character field - decode as EBCDIC
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        row[col_name.lower()] = decoded
                    except:
                        row[col_name.lower()] = ""
            
            rows.append(row)
    
    return pl.DataFrame(rows)


# First, let's examine the file structures
def examine_file_structure(filepath: Path, num_bytes: int = 400):
    """Examine the first few bytes of a file to understand its structure"""
    with open(filepath, 'rb') as f:
        data = f.read(num_bytes)
    
    print(f"\n=== {filepath.name} - First {num_bytes} bytes ===")
    print(f"Hex dump:")
    for i in range(0, min(len(data), num_bytes), 16):
        hex_str = ' '.join(f'{b:02X}' for b in data[i:i+16])
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in data[i:i+16])
        print(f"  {i:04d}: {hex_str}  {ascii_str}")
    
    # Check for common record separators
    if b'\n' in data[:1000]:
        print(f"\nContains newline characters (0x0A)")
    if b'\r\n' in data[:1000]:
        print(f"Contains CRLF (0x0D 0x0A)")
    
    return data


# Examine file structures
print("\n=== Examining file structures ===")
coll_data = examine_file_structure(COLL_FILE, 400)
desc_data = examine_file_structure(DESC_FILE, 400)

# COLL file: Determine record length
coll_file_size = COLL_FILE.stat().st_size
desc_file_size = DESC_FILE.stat().st_size
print(f"\nCOLL file size: {coll_file_size} bytes")
print(f"DESC file size: {desc_file_size} bytes")

# For COLL: Try record length 158 (from SAS positions)
# But check if there might be a longer record length
# Common mainframe record lengths: 80, 133, 158, 256, 512, 1024, 2048, 4096
coll_candidates = [158, 256, 512, 1024, 2048, 4096]
for rec_len in coll_candidates:
    if coll_file_size % rec_len == 0:
        print(f"COLL: Record length {rec_len} divides evenly -> {coll_file_size // rec_len} records")

# For DESC: The positions go up to 298, so minimum is 298
# But try larger record lengths
desc_candidates = [298, 300, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 84750]
for rec_len in desc_candidates:
    if desc_file_size % rec_len == 0:
        print(f"DESC: Record length {rec_len} divides evenly -> {desc_file_size // rec_len} records")

# Let's try to find the record length by looking for patterns
# For DESC, if we expect ~58604 records, the record length should be around:
expected_desc_records = 58604
estimated_rec_len = desc_file_size // expected_desc_records
print(f"\nEstimated DESC record length (based on ~58604 records): {estimated_rec_len}")

# Try common record lengths near the estimate
for rec_len in range(max(298, estimated_rec_len - 100), estimated_rec_len + 100):
    if rec_len > 0 and desc_file_size % rec_len == 0:
        print(f"DESC: Record length {rec_len} divides evenly -> {desc_file_size // rec_len} records")
        break

# Use the correct record lengths
# For COLL: 158 seems correct based on the SAS code
# For DESC: Need to find the right length
COLL_RECORD_LENGTH = 158
DESC_RECORD_LENGTH = estimated_rec_len if estimated_rec_len > 0 else 298

print(f"\nUsing COLL record length: {COLL_RECORD_LENGTH}")
print(f"Using DESC record length: {DESC_RECORD_LENGTH}")

# Read the files
try:
    coll = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
    desc = read_ebcdic_fixed_records(DESC_FILE, DESC_RECORD_LENGTH, desc_specs)
    
    print(f"\nCOLL rows: {coll.height}")
    print(f"DESC rows: {desc.height}")
    
    # Show unique CINSTCL and NATGUAR values
    if desc.height > 0:
        print(f"\nUnique CINSTCL values: {desc['cinstcl'].unique().to_list()[:20]}")
        print(f"Unique NATGUAR values: {desc['natguar'].unique().to_list()[:20]}")
    
    # Check for '18' and '06' values
    if desc.height > 0:
        has_18 = desc.filter(pl.col('cinstcl') == '18').height
        has_06 = desc.filter(pl.col('natguar') == '06').height
        print(f"\nRows with CINSTCL='18': {has_18}")
        print(f"Rows with NATGUAR='06': {has_06}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
