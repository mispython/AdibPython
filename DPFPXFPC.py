import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"              # Input fixed-width file (ASCII)
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet" # Final output parquet
OUTPUT_CSV = "ADDR_SAVINGS.csv"         # Final output CSV

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

# -----------------------------------------------------------
# STEP 0: Analyze the file format
# -----------------------------------------------------------
print("=" * 60)
print("STEP 0: Analyzing file format...")
print("=" * 60)

with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

record_length = 428
num_records = len(raw_data) // record_length

# Analyze first few records
print(f"Record length: {record_length}")
print(f"Total records: {num_records:,}")
print()

for rec_num in range(min(5, num_records)):
    start = rec_num * record_length
    record = raw_data[start:start+record_length]
    
    print(f"Record {rec_num + 1}:")
    print(f"  Full hex (first 100 bytes): {record[:100].hex()}")
    print(f"  ASCII: {record[:100].decode('ascii', errors='replace')}")
    print()
    
    # Check specific fields
    print(f"  BANKNO (0:2): hex={record[0:2].hex()}, ascii='{record[0:2].decode('ascii', errors='replace')}'")
    print(f"  APPCODE (2:3): hex={record[2:3].hex()}, ascii='{record[2:3].decode('ascii', errors='replace')}'")
    print(f"  ACCTNO (3:9): hex={record[3:9].hex()}, ascii='{record[3:9].decode('ascii', errors='replace')}'")
    print(f"  BRANCH (9:13): hex={record[9:13].hex()}, ascii='{record[9:13].decode('ascii', errors='replace')}'")
    print(f"  NAME (13:37): hex={record[13:37].hex()}, ascii='{record[13:37].decode('ascii', errors='replace')}'")
    print(f"  LEDGBAL (83:90): hex={record[83:90].hex()}")
    print(f"  CURBAL (90:97): hex={record[90:97].hex()}")
    print()

# Based on analysis, it seems the "packed decimal" fields might actually be zoned decimal (ASCII numbers)
# Let's check if they're readable as numbers
print("=" * 60)
print("Checking if numeric fields are plain text...")
print("=" * 60)

test_record = raw_data[:record_length]

# Try reading fields as ASCII numbers
try:
    bankno = test_record[0:2].decode('ascii').strip()
    print(f"BANKNO as text: '{bankno}' -> {int(bankno) if bankno else 0}")
except:
    print("BANKNO is not plain text")

try:
    acctno = test_record[3:9].decode('ascii').strip()
    print(f"ACCTNO as text: '{acctno}' -> {int(acctno) if acctno else 0}")
except:
    print("ACCTNO is not plain text")

try:
    branch = test_record[9:13].decode('ascii').strip()
    print(f"BRANCH as text: '{branch}' -> {int(branch) if branch else 0}")
except:
    print("BRANCH is not plain text")

try:
    ledgbal = test_record[83:90].decode('ascii').strip()
    print(f"LEDGBAL as text: '{ledgbal}'")
except:
    print("LEDGBAL is not plain text")

print("\nIt appears the file might be completely ASCII with zoned decimal numbers,")
print("not packed decimal. The SAS PD format might be reading zoned decimal fields.")
