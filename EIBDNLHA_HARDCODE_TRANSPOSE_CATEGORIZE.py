"""
EIBDNLHA - Hardcode Data Transpose and Categorization
1:1 conversion from SAS to Python
Processes hardcoded amounts, transposes by product, and categorizes into different output files
Includes additional product codes '93419' and '94419' with items A1.22 and B1.22
"""

import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# Initialize paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
STORE_DIR = OUTPUT_DIR / "store"
STOREM_DIR = OUTPUT_DIR / "storem"

# Create directories
STORE_DIR.mkdir(parents=True, exist_ok=True)
STOREM_DIR.mkdir(parents=True, exist_ok=True)

# Input file
HARDCODE_FILE = INPUT_DIR / "hardcode.parquet"

# Connect to DuckDB
con = duckdb.connect(":memory:")

print("Processing EIBDNLHA - Hardcode Transpose and Categorization")

# ============================================================================
# DATA HARDCODE - Read and process hardcode file
# Input positions: @007 BNMCODE (14 chars), @026 AMOUNT (10 chars)
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE hardcode_raw AS
    SELECT 
        bnmcode,
        amount,
        SUBSTR(bnmcode, 1, 5) as prod,
        SUBSTR(bnmcode, 9, 1) as indnon
    FROM read_parquet('{HARDCODE_FILE}')
    WHERE row_number() OVER () > 1
""")

# Filter: Delete records where INDNON = '0'
# Apply negative multiplier for specific products (includes '93419','94419')
con.execute("""
    CREATE OR REPLACE TABLE hardcode_filtered AS
    SELECT 
        bnmcode,
        prod,
        indnon,
        CASE 
            WHEN prod IN (
                '93329','93421','93423','93424','93425','93429',
                '93419','94419',
                '94329','94421','94423','94424','94425','94429',
                '95329','95421','95423','95424','95425','95429',
                '96329','96421','96423','96424','96425','96429'
            ) THEN amount * (-1)
            ELSE amount
        END as amount
    FROM hardcode_raw
    WHERE indnon != '0'
""")

# ============================================================================
# PROC TRANSPOSE - Transpose data by PROD
# Pivot amounts into columns: COL1-COL6 (WEEK, MONTH, QTR, HALFYR, YEAR, LAST)
# ============================================================================

# First, add row number within each PROD group to identify which column
con.execute("""
    CREATE OR REPLACE TABLE hardcode_numbered AS
    SELECT 
        prod,
        amount,
        ROW_NUMBER() OVER (PARTITION BY prod ORDER BY bnmcode) as col_num
    FROM hardcode_filtered
""")

# Pivot the data
con.execute("""
    CREATE OR REPLACE TABLE hardcode_transposed AS
    SELECT 
        prod,
        MAX(CASE WHEN col_num = 1 THEN amount END) as week,
        MAX(CASE WHEN col_num = 2 THEN amount END) as month,
        MAX(CASE WHEN col_num = 3 THEN amount END) as qtr,
        MAX(CASE WHEN col_num = 4 THEN amount END) as halfyr,
        MAX(CASE WHEN col_num = 5 THEN amount END) as year,
        MAX(CASE WHEN col_num = 6 THEN amount END) as last
    FROM hardcode_numbered
    GROUP BY prod
""")

# Rename to match SAS naming
con.execute("ALTER TABLE hardcode_transposed RENAME TO hardcode")

# ============================================================================
# PROC PRINT (optional - for display)
# ============================================================================
print("\nTransposed Data:")
transposed_df = con.execute("SELECT * FROM hardcode").df()
print(transposed_df)

# ============================================================================
# DATA STORE - Process and categorize into multiple output datasets
# ============================================================================

# Create the main processed table with ITEM assignments
# NOTE: Includes new mappings for '93419' -> 'A1.22' and '94419' -> 'B1.22'
con.execute("""
    CREATE OR REPLACE TABLE hardcode_processed AS
    SELECT 
        prod,
        COALESCE(week, 0) + COALESCE(month, 0) + COALESCE(qtr, 0) + 
        COALESCE(halfyr, 0) + COALESCE(year, 0) + COALESCE(last, 0) as balance,
        week,
        month,
        qtr,
        halfyr,
        year,
        last,
        CASE 
            WHEN prod = '93229' THEN 'A1.11'
            WHEN prod = '95229' THEN 'A1.11'
            WHEN prod = '94229' THEN 'B1.11'
            WHEN prod = '96229' THEN 'B1.11'
            WHEN prod = '93329' THEN 'A1.20'
            WHEN prod = '93421' THEN 'A1.23'
            WHEN prod = '93423' THEN 'A1.25'
            WHEN prod = '93424' THEN 'A1.27'
            WHEN prod = '93425' THEN 'A1.28'
            WHEN prod = '93429' THEN 'A1.29'
            WHEN prod = '94329' THEN 'B1.20'
            WHEN prod = '94421' THEN 'B1.23'
            WHEN prod = '94423' THEN 'B1.25'
            WHEN prod = '94424' THEN 'B1.27'
            WHEN prod = '94425' THEN 'B1.28'
            WHEN prod = '94429' THEN 'B1.29'
            WHEN prod = '95329' THEN 'A1.20'
            WHEN prod = '95421' THEN 'A1.23'
            WHEN prod = '95423' THEN 'A1.25'
            WHEN prod = '95424' THEN 'A1.27'
            WHEN prod = '95425' THEN 'A1.28'
            WHEN prod = '95429' THEN 'A1.29'
            WHEN prod = '96329' THEN 'B1.20'
            WHEN prod = '96421' THEN 'B1.23'
            WHEN prod = '96423' THEN 'B1.25'
            WHEN prod = '96424' THEN 'B1.27'
            WHEN prod = '96425' THEN 'B1.28'
            WHEN prod = '96429' THEN 'B1.29'
            WHEN prod = '93419' THEN 'A1.22'
            WHEN prod = '94419' THEN 'B1.22'
            WHEN prod = '93635' THEN 'A2.08'
            ELSE NULL
        END as item
    FROM hardcode
""")

# ============================================================================
# OUTPUT to STORE.HARDRMP1 - Products starting with '93' and ITEM starts with 'A' (except A2.08)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE store_hardrmp1 AS
    SELECT *
    FROM hardcode_processed
    WHERE SUBSTR(item, 1, 1) = 'A' 
        AND item != 'A2.08'
        AND SUBSTR(prod, 1, 2) = '93'
""")

hardrmp1 = con.execute("SELECT * FROM store_hardrmp1").arrow()
pq.write_table(hardrmp1, STORE_DIR / "HARDRMP1.parquet")
csv.write_csv(hardrmp1, STORE_DIR / "HARDRMP1.csv")
print(f"\nSaved: STORE.HARDRMP1 ({len(hardrmp1)} records)")

# ============================================================================
# OUTPUT to STORE.HARDRMP2 - Products starting with '95' and ITEM starts with 'A' (except A2.08)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE store_hardrmp2 AS
    SELECT *
    FROM hardcode_processed
    WHERE SUBSTR(item, 1, 1) = 'A' 
        AND item != 'A2.08'
        AND SUBSTR(prod, 1, 2) = '95'
""")

hardrmp2 = con.execute("SELECT * FROM store_hardrmp2").arrow()
pq.write_table(hardrmp2, STORE_DIR / "HARDRMP2.parquet")
csv.write_csv(hardrmp2, STORE_DIR / "HARDRMP2.csv")
print(f"Saved: STORE.HARDRMP2 ({len(hardrmp2)} records)")

# ============================================================================
# OUTPUT to STORE.HARDFXP1 - Products starting with '94' and ITEM starts with 'B' (or A2.08)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE store_hardfxp1 AS
    SELECT *
    FROM hardcode_processed
    WHERE (SUBSTR(item, 1, 1) = 'B' OR item = 'A2.08')
        AND SUBSTR(prod, 1, 2) = '94'
""")

hardfxp1 = con.execute("SELECT * FROM store_hardfxp1").arrow()
pq.write_table(hardfxp1, STORE_DIR / "HARDFXP1.parquet")
csv.write_csv(hardfxp1, STORE_DIR / "HARDFXP1.csv")
print(f"Saved: STORE.HARDFXP1 ({len(hardfxp1)} records)")

# ============================================================================
# OUTPUT to STORE.HARDFXP2 - Products starting with '96' and ITEM starts with 'B' (or A2.08)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE store_hardfxp2 AS
    SELECT *
    FROM hardcode_processed
    WHERE (SUBSTR(item, 1, 1) = 'B' OR item = 'A2.08')
        AND SUBSTR(prod, 1, 2) = '96'
""")

hardfxp2 = con.execute("SELECT * FROM store_hardfxp2").arrow()
pq.write_table(hardfxp2, STORE_DIR / "HARDFXP2.parquet")
csv.write_csv(hardfxp2, STORE_DIR / "HARDFXP2.csv")
print(f"Saved: STORE.HARDFXP2 ({len(hardfxp2)} records)")

# ============================================================================
# OUTPUT to STORE.HARDUTP1 - Product '93635' with ITEM 'A2.08'
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE store_hardutp1 AS
    SELECT *
    FROM hardcode_processed
    WHERE prod = '93635' AND item = 'A2.08'
""")

hardutp1 = con.execute("SELECT * FROM store_hardutp1").arrow()
pq.write_table(hardutp1, STORE_DIR / "HARDUTP1.parquet")
csv.write_csv(hardutp1, STORE_DIR / "HARDUTP1.csv")
print(f"Saved: STORE.HARDUTP1 ({len(hardutp1)} records)")

# ============================================================================
# Copy STORE datasets to STOREM
# ============================================================================
print("\nCopying to STOREM directory...")

# STOREM.HARDRMP1
storem_hardrmp1 = con.execute("SELECT * FROM store_hardrmp1").arrow()
pq.write_table(storem_hardrmp1, STOREM_DIR / "HARDRMP1.parquet")
csv.write_csv(storem_hardrmp1, STOREM_DIR / "HARDRMP1.csv")
print(f"Saved: STOREM.HARDRMP1")

# STOREM.HARDRMP2
storem_hardrmp2 = con.execute("SELECT * FROM store_hardrmp2").arrow()
pq.write_table(storem_hardrmp2, STOREM_DIR / "HARDRMP2.parquet")
csv.write_csv(storem_hardrmp2, STOREM_DIR / "HARDRMP2.csv")
print(f"Saved: STOREM.HARDRMP2")

# STOREM.HARDFXP1
storem_hardfxp1 = con.execute("SELECT * FROM store_hardfxp1").arrow()
pq.write_table(storem_hardfxp1, STOREM_DIR / "HARDFXP1.parquet")
csv.write_csv(storem_hardfxp1, STOREM_DIR / "HARDFXP1.csv")
print(f"Saved: STOREM.HARDFXP1")

# STOREM.HARDFXP2
storem_hardfxp2 = con.execute("SELECT * FROM store_hardfxp2").arrow()
pq.write_table(storem_hardfxp2, STOREM_DIR / "HARDFXP2.parquet")
csv.write_csv(storem_hardfxp2, STOREM_DIR / "HARDFXP2.csv")
print(f"Saved: STOREM.HARDFXP2")

# STOREM.HARDUTP1
storem_hardutp1 = con.execute("SELECT * FROM store_hardutp1").arrow()
pq.write_table(storem_hardutp1, STOREM_DIR / "HARDUTP1.parquet")
csv.write_csv(storem_hardutp1, STOREM_DIR / "HARDUTP1.csv")
print(f"Saved: STOREM.HARDUTP1")

# ============================================================================
# PROC PRINT - Display final STOREM.HARDUTP1 data
# ============================================================================
print("\n" + "="*80)
print("PROC PRINT - STOREM.HARDUTP1")
print("="*80)
hardutp1_df = con.execute("SELECT * FROM store_hardutp1").df()
if len(hardutp1_df) > 0:
    print(hardutp1_df.to_string(index=False))
else:
    print("No records in HARDUTP1")
print("="*80)

# ============================================================================
# Summary Report
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"{'Dataset':<20} {'Records':<10} {'Location':<30}")
print("-"*80)
print(f"{'HARDRMP1':<20} {len(hardrmp1):<10} {'store/ and storem/':<30}")
print(f"{'HARDRMP2':<20} {len(hardrmp2):<10} {'store/ and storem/':<30}")
print(f"{'HARDFXP1':<20} {len(hardfxp1):<10} {'store/ and storem/':<30}")
print(f"{'HARDFXP2':<20} {len(hardfxp2):<10} {'store/ and storem/':<30}")
print(f"{'HARDUTP1':<20} {len(hardutp1):<10} {'store/ and storem/':<30}")
print("="*80)

print("\nKey Differences from EIIDNLHA:")
print("  - Added product codes: '93419' (A1.22), '94419' (B1.22)")
print("  - Input position changed: @026 AMOUNT (was @024)")
print("  - Added PROC PRINT for STOREM.HARDUTP1 at the end")

# Close connection
con.close()
print("\nProcessing complete!")
