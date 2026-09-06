# =========================
# COLL file processing (EBCDIC with packed decimal)
# =========================
print("Processing COLL and DESC files...")

def read_ebcdic_fixed_records(filepath: Path, record_length: int, col_specs: list) -> pl.DataFrame:
    """
    Read EBCDIC file with fixed-length records
    For packed decimal (PD), we need to read the raw bytes and decode them properly
    """
    import struct
    
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
                    # PD6 means 6 bytes of packed decimal
                    raw_bytes = record[start_idx:end_idx]
                    
                    # Decode packed decimal
                    # Each byte contains two nibbles (4 bits each)
                    # The last nibble contains the sign
                    try:
                        # Convert bytes to hex string
                        hex_str = raw_bytes.hex()
                        
                        # Remove the sign nibble (last character)
                        digits = hex_str[:-1]
                        
                        # Check sign nibble
                        sign_nibble = hex_str[-1].upper()
                        
                        # Parse the digits
                        if digits:
                            value = int(digits)
                            # Apply sign
                            if sign_nibble in ('D', 'B'):  # Negative in EBCDIC
                                value = -value
                            row[col_name.lower()] = float(value)
                        else:
                            row[col_name.lower()] = None
                    except Exception as e:
                        row[col_name.lower()] = None
                        
                elif col_type == 'numeric':
                    # Regular numeric field - decode as EBCDIC then convert to float
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        row[col_name.lower()] = float(decoded) if decoded else None
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


# COLL file: Need to determine the actual record length
# From SAS code: @004 CCOLLNO PD6. @146 ACCTNO PD6. @153 NOTENO PD6.
# The maximum position is 158 (153 + 6 - 1), so record length is at least 158
# Let's try different record lengths to find the right one
COLL_RECORD_LENGTH = 158  # Minimum length based on positions
DESC_RECORD_LENGTH = 298  # @291 TRANCHE $8. means position 291-298

# Read COLL file with packed decimal parsing
coll_specs = [
    ("ccollno", 4, 9, "pd"),    # @004 CCOLLNO PD6.
    ("acctno", 146, 151, "pd"),  # @146 ACCTNO PD6.
    ("noteno", 153, 158, "pd")   # @153 NOTENO PD6.
]

# Read DESC file with correct column positions
desc_specs = [
    ("ccollno", 1, 11, "numeric"),    # @001 CCOLLNO 11.
    ("cinstcl", 51, 52, "character"),  # @051 CINSTCL $2.
    ("natguar", 55, 56, "character"),  # @055 NATGUAR $2.
    ("census", 211, 220, "numeric"),   # @211 CENSUS 10.
    ("tranche", 291, 298, "character") # @291 TRANCHE $8.
]

try:
    # First, let's check the file size to determine record length
    coll_file_size = COLL_FILE.stat().st_size
    desc_file_size = DESC_FILE.stat().st_size
    
    print(f"COLL file size: {coll_file_size} bytes")
    print(f"DESC file size: {desc_file_size} bytes")
    
    # Try reading with different record lengths for COLL
    # Common record lengths might be 160, 256, 320, 512, etc.
    for rec_len in [158, 160, 256, 320, 512]:
        if coll_file_size % rec_len == 0:
            print(f"COLL record length {rec_len} divides evenly ({coll_file_size // rec_len} records)")
            COLL_RECORD_LENGTH = rec_len
            break
    
    for rec_len in [298, 300, 512, 1024]:
        if desc_file_size % rec_len == 0:
            print(f"DESC record length {rec_len} divides evenly ({desc_file_size // rec_len} records)")
            DESC_RECORD_LENGTH = rec_len
            break
    
    # Read COLL with fixed record length
    coll = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
    print(f"  COLL rows: {coll.height}")
    
    # Read DESC with fixed record length
    desc = read_ebcdic_fixed_records(DESC_FILE, DESC_RECORD_LENGTH, desc_specs)
    print(f"  DESC rows: {desc.height}")
    
    # Print sample data for debugging
    print("\n=== COLL Data Sample (first 5 rows) ===")
    print(coll.head(5))
    
    print("\n=== DESC Data Sample (first 5 rows) ===")
    print(desc.head(5))
    
    # Check unique values in DESC for CINSTCL and NATGUAR
    if desc.height > 0:
        if 'cinstcl' in desc.columns:
            unique_cinstcl = desc['cinstcl'].unique().to_list()
            print(f"\nUnique CINSTCL values (first 20): {unique_cinstcl[:20]}")
        if 'natguar' in desc.columns:
            unique_natguar = desc['natguar'].unique().to_list()
            print(f"Unique NATGUAR values (first 20): {unique_natguar[:20]}")
    
    # Convert ccollno to consistent type (float64) for joining
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    
    # Convert acctno and noteno to float64
    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Float64).alias("acctno"),
        pl.col("noteno").cast(pl.Float64).alias("noteno")
    ])
    
except Exception as e:
    print(f"Warning: Error reading EBCDIC files: {e}")
    import traceback
    traceback.print_exc()
    print("Creating empty DataFrames as placeholder")
    coll = pl.DataFrame(schema={"ccollno": pl.Float64, "acctno": pl.Float64, "noteno": pl.Float64})
    desc = pl.DataFrame(schema={"ccollno": pl.Float64, "cinstcl": pl.Utf8, "natguar": pl.Utf8, 
                                "census": pl.Float64, "tranche": pl.Utf8})

print(f"\n  COLL rows: {coll.height}")
print(f"  DESC rows: {desc.height}")

# PROC SORT; BY CCOLLNO; (for both COLL and DESC)
coll = coll.sort(by="ccollno")
desc = desc.sort(by="ccollno")

# DATA COLL; MERGE COLL(IN=A) DESC(IN=B); BY CCOLLNO; IF A AND B;
coll = coll.join(desc, on="ccollno", how="inner")
print(f"  COLL rows after join with DESC: {coll.height}")

# IF CINSTCL='18' AND NATGUAR='06';
coll = coll.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))

# PROC SORT; BY ACCTNO NOTENO;
coll = coll.sort(by=["acctno", "noteno"])

print(f"\n  COLL rows after filter: {coll.height}")
