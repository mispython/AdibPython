# =========================
# COLL and DESC processing - ENHANCED DIAGNOSTIC
# =========================
print("Processing COLL and DESC files...")

coll_specs = [
    ("ccollno", 4, 9, "pd"),
    ("acctno", 146, 151, "pd"),
    ("noteno", 153, 158, "pd")
]

desc_specs = [
    ("ccollno", 1, 11, "numeric"),
    ("cinstcl", 51, 52, "character"),
    ("natguar", 55, 56, "character"),
    ("census", 211, 220, "numeric"),
    ("tranche", 291, 298, "character")
]

COLL_RECORD_LENGTH = 158
desc_file_size = DESC_FILE.stat().st_size
expected_desc_records = 58604
DESC_RECORD_LENGTH = desc_file_size // expected_desc_records

print(f"COLL record length: {COLL_RECORD_LENGTH}")
print(f"DESC record length: {DESC_RECORD_LENGTH}")

# ==========================================
# ENHANCED DIAGNOSTIC: Scan DESC file for non-empty records
# ==========================================
print("\n=== Scanning DESC file for non-empty records ===")

# Scan the first 10,000 records to find non-empty ones
non_empty_records = []
total_scanned = 10000

with open(DESC_FILE, 'rb') as f:
    for i in range(total_scanned):
        record = f.read(DESC_RECORD_LENGTH)
        if not record or len(record) < DESC_RECORD_LENGTH:
            break
        
        # Check if record has non-space data
        decoded = record.decode('cp037', errors='ignore')
        # Check if the record has any non-space characters
        has_data = any(c not in (' ', '\x00', '\x40') for c in decoded[:200])
        
        if has_data:
            non_empty_records.append((i, decoded))
            
            # Show first 200 chars of first 5 non-empty records
            if len(non_empty_records) <= 5:
                print(f"\nRecord {i} (0-based index):")
                print(f"  First 200 chars: [{decoded[:200]}]")
                
                # Look for '18' and '06' in first 500 chars
                positions_18 = [j+1 for j in range(min(len(decoded)-1, 500)) if decoded[j:j+2] == '18']
                positions_06 = [j+1 for j in range(min(len(decoded)-1, 500)) if decoded[j:j+2] == '06']
                
                if positions_18:
                    print(f"  '18' found at positions: {positions_18[:10]}")
                if positions_06:
                    print(f"  '06' found at positions: {positions_06[:10]}")
                
                # Show CCOLLNO
                print(f"  CCOLLNO: '{decoded[0:11].strip()}'")
                
                # Show positions 51-52 and 55-56
                print(f"  Pos 51-52: '{decoded[50:52]}'")
                print(f"  Pos 55-56: '{decoded[54:56]}'")

print(f"\nTotal non-empty records found in first {total_scanned} records: {len(non_empty_records)}")

# Now check if the DESC file might be line-delimited instead
print("\n=== Checking if DESC is line-delimited ===")
with open(DESC_FILE, 'rb') as f:
    # Read first 1000 bytes
    first_chunk = f.read(1000)

# Check for newline characters
newline_positions = [i for i, b in enumerate(first_chunk) if b == 0x0A or b == 0x0D]
if newline_positions:
    print(f"Found newline characters at positions: {newline_positions[:20]}")
else:
    print("No newline characters found in first 1000 bytes")

# Check if DESC might have a much smaller record length
# Try record length 298 (from SAS code)
print("\n=== Checking record length 298 ===")
with open(DESC_FILE, 'rb') as f:
    record_298 = f.read(298)
    decoded_298 = record_298.decode('cp037', errors='ignore')
    print(f"First 298 chars: [{decoded_298}]")

# Try reading DESC as line-delimited text
print("\n=== Reading DESC as line-delimited ===")
try:
    with open(DESC_FILE, 'r', encoding='cp037', errors='ignore') as f:
        lines = []
        for i, line in enumerate(f):
            if i >= 10:
                break
            lines.append(line.rstrip('\n\r'))
            if line.strip():
                print(f"Line {i}: [{line[:200]}]")
except Exception as e:
    print(f"Error reading as text: {e}")

print("\n=== End Enhanced Diagnostic ===")
# ==========================================
