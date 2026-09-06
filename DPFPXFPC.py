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
    records_read = 0
    
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
            records_read += 1
            
            # Safety check
            if records_read > 70000:  # Don't read more than expected
                break
    
    return pl.DataFrame(rows)


# Define column specifications
coll_specs = [
    ("ccollno", 4, 9, "pd"),    # @004 CCOLLNO PD6.
    ("acctno", 146, 151, "pd"),  # @146 ACCTNO PD6.
    ("noteno", 153, 158, "pd")   # @153 NOTENO PD6.
]

desc_specs = [
    ("ccollno", 1, 11, "numeric"),    # @001 CCOLLNO 11.
    ("cinstcl", 51, 52, "character"),  # @051 CINSTCL $2.
    ("natguar", 55, 56, "character"),  # @055 NATGUAR $2.
    ("census", 211, 220, "numeric"),   # @211 CENSUS 10.
    ("tranche", 291, 298, "character") # @291 TRANCHE $8.
]

# COLL: Record length 158 (confirmed from earlier analysis)
COLL_RECORD_LENGTH = 158

# DESC: Calculate record length from file size and expected records
desc_file_size = DESC_FILE.stat().st_size
expected_desc_records = 58604  # From earlier analysis
DESC_RECORD_LENGTH = desc_file_size // expected_desc_records

print(f"DESC file size: {desc_file_size} bytes")
print(f"Expected DESC records: {expected_desc_records}")
print(f"Calculated DESC record length: {DESC_RECORD_LENGTH}")

# Verify the record length divides evenly
if desc_file_size % DESC_RECORD_LENGTH == 0:
    actual_records = desc_file_size // DESC_RECORD_LENGTH
    print(f"Record length {DESC_RECORD_LENGTH} -> {actual_records} records")
else:
    print(f"Warning: Record length {DESC_RECORD_LENGTH} does not divide evenly")
    print(f"Remainder: {desc_file_size % DESC_RECORD_LENGTH}")

try:
    # Read COLL
    print("\nReading COLL file...")
    coll = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
    print(f"COLL rows: {coll.height}")
    
    # Read DESC
    print("Reading DESC file...")
    desc = read_ebcdic_fixed_records(DESC_FILE, DESC_RECORD_LENGTH, desc_specs)
    print(f"DESC rows: {desc.height}")
    
    # Print sample data
    print("\n=== COLL Data Sample (first 3 rows) ===")
    print(coll.head(3))
    
    print("\n=== DESC Data Sample (first 3 rows) ===")
    print(desc.head(3))
    
    # Check unique values
    if desc.height > 0:
        unique_cinstcl = desc['cinstcl'].unique().to_list()
        print(f"\nUnique CINSTCL values (first 20): {unique_cinstcl[:20]}")
        unique_natguar = desc['natguar'].unique().to_list()
        print(f"Unique NATGUAR values (first 20): {unique_natguar[:20]}")
        
        # Check for '18' and '06'
        has_18 = desc.filter(pl.col('cinstcl') == '18').height
        has_06 = desc.filter(pl.col('natguar') == '06').height
        print(f"Rows with CINSTCL='18': {has_18}")
        print(f"Rows with NATGUAR='06': {has_06}")
    
    # Convert ccollno to consistent type
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    
    # Convert acctno and noteno to float64
    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Float64).alias("acctno"),
        pl.col("noteno").cast(pl.Float64).alias("noteno")
    ])
    
    # Sort and join
    coll = coll.sort(by="ccollno")
    desc = desc.sort(by="ccollno")
    
    coll_joined = coll.join(desc, on="ccollno", how="inner")
    print(f"\nCOLL rows after join: {coll_joined.height}")
    
    # Filter
    coll_filtered = coll_joined.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))
    print(f"COLL rows after filter: {coll_filtered.height}")
    
    # Use the filtered data
    coll = coll_filtered
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    # Create empty placeholders
    coll = pl.DataFrame(schema={"ccollno": pl.Float64, "acctno": pl.Float64, "noteno": pl.Float64, 
                                "cinstcl": pl.Utf8, "natguar": pl.Utf8, "census": pl.Float64, "tranche": pl.Utf8})

# Continue with the rest of the processing...
print(f"\nFinal COLL rows: {coll.height}")
