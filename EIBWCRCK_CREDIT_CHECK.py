# EIBWCRCK_CREDIT_CHECK.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path

# Configuration
base_path = Path(".")
loan_path = base_path / "LOAN"
ln_path = base_path / "LN"
output_path = base_path / "OUTPUT"

# Create output directory
output_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIBWCRCK - Credit Reconciliation Check")

# Step 1: Process REPTDATE (equivalent to DATA REPTDATE)
reptdate_query = f"SELECT * FROM read_parquet('{loan_path / \"REPTDATE.parquet\"}')"
reptdate_df = conn.execute(reptdate_query).arrow()

if len(reptdate_df) > 0:
    reptdate = reptdate_df['REPTDATE'][0].as_py()
    rept_day = reptdate.day
    
    if rept_day == 8:
        nowk = '1'
    elif rept_day == 15:
        nowk = '2' 
    elif rept_day == 22:
        nowk = '3'
    else:
        nowk = '4'
        
    reptyear = reptdate.strftime('%y')
    reptmon = reptdate.strftime('%m')
else:
    nowk, reptyear, reptmon = '1', '25', '01'

# Step 2: Process CRIS data (equivalent to DATA CRIS)
cred_file = base_path / "CRED.txt"
if cred_file.exists():
    cris_query = f"""
    SELECT 
        CAST(SUBSTR(line, 1, 9) as INTEGER) as FICODE,
        CAST(SUBSTR(line, 10, 3) as INTEGER) as APCODE,
        CAST(SUBSTR(line, 13, 10) as INTEGER) as ACCTNO,
        CAST(SUBSTR(line, 43, 10) as INTEGER) as NOTENO,
        CAST(SUBSTR(line, 81, 16) as DOUBLE) as OUTSTAND,
        SUBSTR(line, 120, 1) as ACCTSTAT,
        CAST(SUBSTR(line, 165, 4) as INTEGER) as COSTCTR,
        CAST(SUBSTR(line, 182, 5) as INTEGER) as FACILITY,
        CAST(SUBSTR(line, 321, 17) as DOUBLE) as CURBALCR
    FROM read_csv('{cred_file}', header=false, columns={{'line': 'VARCHAR'}})
    WHERE LENGTH(line) >= 337
      AND SUBSTR(line, 120, 1) NOT IN ('S','W','F','K')
      AND CAST(SUBSTR(line, 165, 4) as INTEGER) != 901
    """
    cris_df = conn.execute(cris_query).arrow()
else:
    cris_df = pa.table({
        'FICODE': pa.array([], pa.int32()),
        'APCODE': pa.array([], pa.int32()),
        'ACCTNO': pa.array([], pa.int32()),
        'NOTENO': pa.array([], pa.int32()),
        'OUTSTAND': pa.array([], pa.float64()),
        'ACCTSTAT': pa.array([], pa.string()),
        'COSTCTR': pa.array([], pa.int32()),
        'FACILITY': pa.array([], pa.int32()),
        'CURBALCR': pa.array([], pa.float64())
    })

# Sort CRIS (equivalent to PROC SORT)
cris_sorted = conn.execute("SELECT * FROM cris_df ORDER BY ACCTNO, NOTENO").arrow()

# Step 3: Load and sort LN data (equivalent to PROC SORT DATA=LN.LOAN&REPTMON&NOWK)
ln_file = ln_path / f"LOAN{reptmon}{nowk}.parquet"
if ln_file.exists():
    ln_query = f"SELECT * FROM read_parquet('{ln_file}')"
    ln_df = conn.execute(ln_query).arrow()
    ln_sorted = conn.execute("SELECT * FROM ln_df ORDER BY ACCTNO, NOTENO").arrow()
else:
    ln_sorted = pa.table({
        'ACCTNO': pa.array([], pa.int32()),
        'NOTENO': pa.array([], pa.int32()),
        'BALANCE': pa.array([], pa.float64())
    })

# Step 4: Merge and create outputs (equivalent to DATA CRED LNCRED ODCRED)
merge_query = """
SELECT 
    COALESCE(l.ACCTNO, c.ACCTNO) as ACCTNO,
    COALESCE(l.NOTENO, c.NOTENO) as NOTENO,
    l.BALANCE,
    c.OUTSTAND,
    c.FACILITY,
    (l.BALANCE - c.OUTSTAND) as DIFF
FROM ln_sorted l
INNER JOIN cris_sorted c ON l.ACCTNO = c.ACCTNO AND l.NOTENO = c.NOTENO
WHERE ABS(l.BALANCE - c.OUTSTAND) >= 1
"""
cred_df = conn.execute(merge_query).arrow()

# Split into LNCRED and ODCRED
lncred_df = conn.execute("SELECT * FROM cred_df WHERE ACCTNO < 3000000000 OR ACCTNO > 3999999999").arrow()
odcred_df = conn.execute("SELECT * FROM cred_df WHERE ACCTNO BETWEEN 3000000000 AND 3999999999").arrow()

# Save outputs
pq.write_table(cred_df, output_path / "CRED.parquet")
csv.write_csv(cred_df, output_path / "CRED.csv")
pq.write_table(lncred_df, output_path / "LNCRED.parquet") 
csv.write_csv(lncred_df, output_path / "LNCRED.csv")
pq.write_table(odcred_df, output_path / "ODCRED.parquet")
csv.write_csv(odcred_df, output_path / "ODCRED.csv")

# Step 5: Error check (equivalent to %MACRO PROCESS)
if len(cred_df) >= 10:
    print(f"ERROR: {len(cred_df)} ACCOUNTS ARE NOT TALLY")
    exit(77)

print(f"Completed: {len(cred_df)} discrepancy records found")
conn.close()
