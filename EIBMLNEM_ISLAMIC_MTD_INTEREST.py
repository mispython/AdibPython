# EIBMLNEM_ISLAMIC_MTD_INTEREST.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime

# Configuration
base_path = Path(".")
input_path = base_path / "input"
loan_path = base_path / "LOAN"
iloan_path = base_path / "ILOAN"

# Create output directories
loan_path.mkdir(exist_ok=True)
iloan_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIBMLNEM - Islamic Account MTD Interest Extractor")

# Step 1: Process REPTDATE (equivalent to DATA LOAN.REPTDATE)
date_file = input_path / "DATEFILE.txt"
if date_file.exists():
    date_query = f"""
    WITH raw_data AS (
        SELECT SUBSTR(line, 1, 11) as EXTDATE_STR
        FROM read_csv('{date_file}', header=false, columns={{'line': 'VARCHAR'}})
        LIMIT 1
    )
    SELECT CAST(
        SUBSTR(EXTDATE_STR, 5, 4) || '-' ||
        SUBSTR(EXTDATE_STR, 1, 2) || '-' ||
        SUBSTR(EXTDATE_STR, 3, 2) 
    as DATE) as REPTDATE
    FROM raw_data
    WHERE LENGTH(EXTDATE_STR) >= 8
    """
    reptdate_df = conn.execute(date_query).arrow()
else:
    reptdate_df = pa.table({'REPTDATE': pa.array([], pa.date32())})

pq.write_table(reptdate_df, loan_path / "REPTDATE.parquet")
csv.write_csv(reptdate_df, loan_path / "REPTDATE.csv")

# Step 2: Process LOAN data (equivalent to DATA LOAN)
acct_file = input_path / "ACCTFILE.txt"
if acct_file.exists():
    loan_query = f"""
    SELECT 
        CAST(SUBSTR(line, 1, 6) as INTEGER) as ACCTNO,
        CAST(SUBSTR(line, 81, 3) as INTEGER) as NOTENO,
        SUBSTR(line, 84, 1) as REVERSED,
        CAST(SUBSTR(line, 85, 2) as INTEGER) as LOANTYPE,
        CAST(SUBSTR(line, 238, 4) as INTEGER) as COSTCTR,
        CAST(SUBSTR(line, 665, 8) as DOUBLE) / 10000000 as MTDINT
    FROM read_csv('{acct_file}', header=false, columns={{'line': 'VARCHAR'}})
    WHERE LENGTH(line) >= 672
    """
    loan_df = conn.execute(loan_query).arrow()
else:
    loan_df = pa.table({
        'ACCTNO': pa.array([], pa.int32()),
        'NOTENO': pa.array([], pa.int32()),
        'REVERSED': pa.array([], pa.string()),
        'LOANTYPE': pa.array([], pa.int32()),
        'COSTCTR': pa.array([], pa.int32()),
        'MTDINT': pa.array([], pa.float64())
    })

# Step 3: Sort and save MTDINT (equivalent to PROC SORT)
loan_sorted = conn.execute("SELECT * FROM loan_df ORDER BY ACCTNO, NOTENO").arrow()
pq.write_table(loan_sorted, loan_path / "MTDINT.parquet")
csv.write_csv(loan_sorted, loan_path / "MTDINT.csv")

# Step 4: Extract Islamic accounts (equivalent to DATA ILOAN.MTDINT)
islamic_df = conn.execute("SELECT * FROM loan_sorted WHERE COSTCTR BETWEEN 3000 AND 4999").arrow()
pq.write_table(islamic_df, iloan_path / "MTDINT.parquet")
csv.write_csv(islamic_df, iloan_path / "MTDINT.csv")

print(f"Completed: {len(loan_sorted)} total records, {len(islamic_df)} Islamic records")
conn.close()
