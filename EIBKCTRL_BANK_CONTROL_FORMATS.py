# EIBKCTRL_BANK_CONTROL_FORMATS.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path

"""
EIBKCTRL - Bank Control Format Builder
Creates standardized formats for occupation and demographic codes used across banking systems
"""

# Configuration using pathlib
base_path = Path(".")
occup_path = base_path / "OCCUP"
misc_path = base_path / "MISC" 
bkctrl_path = base_path / "BKCTRL"

# Create output directory
bkctrl_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIBKCTRL - Bank Control Format Builder")
print("=" * 60)

# Step 1: Process OCCUP data - Occupation codes
print("Processing occupation codes...")

occup_file = occup_path / "OCCUP.txt"
if occup_file.exists():
    # Read fixed-width occupation file
    occup_query = f"""
    WITH raw_data AS (
        SELECT 
            SUBSTR(line, 11, 3) as START,
            SUBSTR(line, 15, 25) as LABEL,
            'C' as TYPE,
            'OCCUPFMT' as FMTNAME
        FROM read_csv('{occup_file}', header=false, columns={{'line': 'VARCHAR'}})
        WHERE LENGTH(line) >= 40
    )
    SELECT 
        TRIM(START) as START,
        TRIM(LABEL) as LABEL,
        TYPE,
        FMTNAME
    FROM raw_data
    WHERE START IS NOT NULL AND START != ''
    """
    occup_df = conn.execute(occup_query).arrow()
    
    # Remove duplicates (equivalent to PROC SORT NODUPKEY)
    occup_dedup_query = """
    SELECT DISTINCT START, LABEL, TYPE, FMTNAME
    FROM occup_df
    ORDER BY START
    """
    occup_final = conn.execute(occup_dedup_query).arrow()
    
    # Write output
    pq.write_table(occup_final, bkctrl_path / "OCCUP.parquet")
    csv.write_csv(occup_final, bkctrl_path / "OCCUP.csv")
    
    print(f"✓ Processed {len(occup_final)} occupation codes")
else:
    print(f"⚠ Warning: OCCUP file not found: {occup_file}")
    occup_final = pa.table({
        'START': pa.array([], pa.string()),
        'LABEL': pa.array([], pa.string()),
        'TYPE': pa.array([], pa.string()),
        'FMTNAME': pa.array([], pa.string())
    })

# Step 2: Process MISC data - Demographic codes
print("Processing demographic codes...")

misc_file = misc_path / "CUSTMSCD.DESC.txt"
if misc_file.exists():
    # Read fixed-width miscellaneous codes file
    misc_query = f"""
    WITH raw_data AS (
        SELECT 
            SUBSTR(line, 5, 2) as DEMONO,
            SUBSTR(line, 8, 10) as DEMOCODE,
            SUBSTR(line, 19, 7) as ENTRYDATE,
            SUBSTR(line, 27, 13) as EFFECTDATE,
            SUBSTR(line, 41, 1) as DELIND,
            SUBSTR(line, 43, 20) as LABEL,
            CASE 
                WHEN SUBSTR(line, 5, 2) = '08' THEN SUBSTR(SUBSTR(line, 8, 10), 2, 2)
                ELSE NULL 
            END as START,
            CASE 
                WHEN SUBSTR(line, 5, 2) = '08' THEN 'C'
                ELSE NULL 
            END as TYPE,
            CASE 
                WHEN SUBSTR(line, 5, 2) = '08' THEN 'BGCFMT'
                ELSE NULL 
            END as FMTNAME
        FROM read_csv('{misc_file}', header=false, columns={{'line': 'VARCHAR'}})
        WHERE LENGTH(line) >= 62
    )
    SELECT 
        TRIM(DEMONO) as DEMONO,
        TRIM(DEMOCODE) as DEMOCODE,
        TRIM(ENTRYDATE) as ENTRYDATE,
        TRIM(EFFECTDATE) as EFFECTDATE,
        TRIM(DELIND) as DELIND,
        TRIM(LABEL) as LABEL,
        TRIM(START) as START,
        TYPE,
        FMTNAME
    FROM raw_data
    WHERE DEMONO IS NOT NULL AND DEMOCODE != ''
    """
    misc_df = conn.execute(misc_query).arrow()
    
    # Remove duplicates (equivalent to PROC SORT NODUPKEY)
    misc_dedup_query = """
    SELECT DISTINCT DEMONO, DEMOCODE, START, TYPE, LABEL, FMTNAME
    FROM misc_df
    ORDER BY DEMONO, DEMOCODE, START
    """
    misc_final = conn.execute(misc_dedup_query).arrow()
    
    # Write output
    pq.write_table(misc_final, bkctrl_path / "MISC.parquet")
    csv.write_csv(misc_final, bkctrl_path / "MISC.csv")
    
    print(f"✓ Processed {len(misc_final)} demographic codes")
    
    # Filter for BGCFMT records (DEMONO = '08')
    bgc_records = conn.execute("""
    SELECT START, LABEL, TYPE, FMTNAME
    FROM misc_final
    WHERE FMTNAME = 'BGCFMT' AND START IS NOT NULL AND START != ''
    """).arrow()
    print(f"✓ Extracted {len(bgc_records)} BGC format records")
    
else:
    print(f"⚠ Warning: MISC file not found: {misc_file}")
    misc_final = pa.table({
        'DEMONO': pa.array([], pa.string()),
        'DEMOCODE': pa.array([], pa.string()),
        'ENTRYDATE': pa.array([], pa.string()),
        'EFFECTDATE': pa.array([], pa.string()),
        'DELIND': pa.array([], pa.string()),
        'LABEL': pa.array([], pa.string()),
        'START': pa.array([], pa.string()),
        'TYPE': pa.array([], pa.string()),
        'FMTNAME': pa.array([], pa.string())
    })
    bgc_records = pa.table({
        'START': pa.array([], pa.string()),
        'LABEL': pa.array([], pa.string()),
        'TYPE': pa.array([], pa.string()),
        'FMTNAME': pa.array([], pa.string())
    })

# Step 3: Create CISFMT combined format catalog
print("Creating combined format catalog...")

# Combine OCCUP and BGC records
cisfmt_query = """
WITH combined_data AS (
    SELECT 
        START,
        LABEL,
        TYPE,
        FMTNAME
    FROM occup_final
    WHERE FMTNAME IS NOT NULL AND FMTNAME != ''
    
    UNION ALL
    
    SELECT 
        START,
        LABEL,
        TYPE,
        FMTNAME
    FROM bgc_records
    WHERE FMTNAME IS NOT NULL AND FMTNAME != ''
)
SELECT 
    START,
    LABEL,
    TYPE,
    FMTNAME
FROM combined_data
WHERE START IS NOT NULL AND START != ''
ORDER BY FMTNAME, START
"""
cisfmt_df = conn.execute(cisfmt_query).arrow()

# Write CISFMT output
pq.write_table(cisfmt_df, bkctrl_path / "CISFMT.parquet")
csv.write_csv(cisfmt_df, bkctrl_path / "CISFMT.csv")

# Step 4: Create format lookup tables for downstream use
print("Creating format lookup tables...")

# Create OCCUPFMT lookup
occup_lookup_query = """
SELECT 
    'OCCUPFMT' as FORMAT_NAME,
    START as CODE,
    LABEL as DESCRIPTION,
    TYPE as DATA_TYPE
FROM cisfmt_df
WHERE FMTNAME = 'OCCUPFMT'
ORDER BY CODE
"""
occup_lookup_df = conn.execute(occup_lookup_query).arrow()

if len(occup_lookup_df) > 0:
    pq.write_table(occup_lookup_df, bkctrl_path / "OCCUPFMT_LOOKUP.parquet")
    csv.write_csv(occup_lookup_df, bkctrl_path / "OCCUPFMT_LOOKUP.csv")
    print(f"✓ Created OCCUPFMT lookup with {len(occup_lookup_df)} codes")

# Create BGCFMT lookup
bgc_lookup_query = """
SELECT 
    'BGCFMT' as FORMAT_NAME,
    START as CODE,
    LABEL as DESCRIPTION,
    TYPE as DATA_TYPE
FROM cisfmt_df
WHERE FMTNAME = 'BGCFMT'
ORDER BY CODE
"""
bgc_lookup_df = conn.execute(bgc_lookup_query).arrow()

if len(bgc_lookup_df) > 0:
    pq.write_table(bgc_lookup_df, bkctrl_path / "BGCFMT_LOOKUP.parquet")
    csv.write_csv(bgc_lookup_df, bkctrl_path / "BGCFMT_LOOKUP.csv")
    print(f"✓ Created BGCFMT lookup with {len(bgc_lookup_df)} codes")

# Step 5: Print summary statistics
print("\n" + "=" * 60)
print("BANK CONTROL FORMATS - SUMMARY")
print("=" * 60)

# Count records by format type
format_summary = conn.execute("""
SELECT 
    FMTNAME as FORMAT_NAME,
    COUNT(*) as RECORD_COUNT,
    MIN(START) as MIN_CODE,
    MAX(START) as MAX_CODE
FROM cisfmt_df
GROUP BY FMTNAME
ORDER BY RECORD_COUNT DESC
""").arrow()

for i in range(len(format_summary)):
    fmt_name = format_summary['FORMAT_NAME'][i].as_py()
    count = format_summary['RECORD_COUNT'][i].as_py()
    min_code = format_summary['MIN_CODE'][i].as_py()
    max_code = format_summary['MAX_CODE'][i].as_py()
    print(f"  {fmt_name:10}: {count:4} records, Codes: {min_code} to {max_code}")

# Sample records
print(f"\nSample Occupation Codes:")
sample_occup = conn.execute("""
SELECT START as CODE, LABEL as DESCRIPTION
FROM cisfmt_df
WHERE FMTNAME = 'OCCUPFMT'
ORDER BY START
LIMIT 5
""").arrow()

for i in range(len(sample_occup)):
    code = sample_occup['CODE'][i].as_py()
    desc = sample_occup['DESCRIPTION'][i].as_py()
    print(f"    {code}: {desc}")

if len(bgc_lookup_df) > 0:
    print(f"\nSample BGC Codes:")
    sample_bgc = conn.execute("""
    SELECT START as CODE, LABEL as DESCRIPTION
    FROM cisfmt_df
    WHERE FMTNAME = 'BGCFMT'
    ORDER BY START
    LIMIT 5
    """).arrow()
    
    for i in range(len(sample_bgc)):
        code = sample_bgc['CODE'][i].as_py()
        desc = sample_bgc['DESCRIPTION'][i].as_py()
        print(f"    {code}: {desc}")

conn.close()

print(f"\n✓ Bank Control Format Builder Completed Successfully!")
print(f"✓ Output files created in: {bkctrl_path}")
print(f"✓ Format lookups ready for SQL joins in downstream processing")
