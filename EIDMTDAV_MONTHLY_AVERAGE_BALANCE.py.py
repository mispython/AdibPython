#!/usr/bin/env python3
"""
EIDMTDAV - Monthly Average Balance Calculation
1:1 conversion from SAS to Python
Processes daily account data (Saving, UMA, FD, Current, Vostro) 
and calculates month-to-date average balances by account
"""

import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from datetime import datetime

# Initialize paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
DP_DIR = INPUT_DIR / "dp"
MIS_DIR = OUTPUT_DIR / "mis"

# Create directories
MIS_DIR.mkdir(parents=True, exist_ok=True)

# Input files
REPTDATE_FILE = DP_DIR / "REPTDATE.parquet"
SAVING_FILE = DP_DIR / "SAVING.parquet"
UMA_FILE = DP_DIR / "UMA.parquet"
FD_FILE = DP_DIR / "FD.parquet"
CURRENT_FILE = DP_DIR / "CURRENT.parquet"
VOSTRO_FILE = DP_DIR / "VOSTRO.parquet"

# Connect to DuckDB
con = duckdb.connect(":memory:")

print("Processing EIDMTDAV - Monthly Average Balance Calculation")

# ============================================================================
# DATA REPTDATE - Read report date and create macro variables
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE reptdate AS
    SELECT reptdate
    FROM read_parquet('{REPTDATE_FILE}')
""")

result = con.execute("SELECT reptdate FROM reptdate").fetchone()
REPTDATE = result[0]

# Extract date components for macro variables
if isinstance(REPTDATE, (int, float)):
    # If REPTDATE is numeric, convert to date
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    # If string, parse it
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)
RDATE = REPTDATE

print(f"Report Date: {REPTDATE}")
print(f"Report Month: {REPTMON}")
print(f"Report Day: {REPTDAY}")

# ============================================================================
# DATA CURSA - Combine Saving and UMA datasets
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE cursa AS
    SELECT 
        branch,
        acctno,
        curbal,
        openind,
        product,
        curcode,
        DATE '{RDATE}' as reptdate
    FROM read_parquet('{SAVING_FILE}')
    
    UNION ALL
    
    SELECT 
        branch,
        acctno,
        curbal,
        openind,
        product,
        curcode,
        DATE '{RDATE}' as reptdate
    FROM read_parquet('{UMA_FILE}')
""")

print(f"CURSA records created: {con.execute('SELECT COUNT(*) FROM cursa').fetchone()[0]}")

# ============================================================================
# DATA CURFD - Process FD dataset
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE curfd AS
    SELECT 
        branch,
        acctno,
        curbal,
        forbal,
        openind,
        product,
        curcode,
        DATE '{RDATE}' as reptdate
    FROM read_parquet('{FD_FILE}')
""")

print(f"CURFD records created: {con.execute('SELECT COUNT(*) FROM curfd').fetchone()[0]}")

# ============================================================================
# DATA CURCA - Combine Current and Vostro datasets
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE curca AS
    SELECT 
        branch,
        acctno,
        curbal,
        forbal,
        openind,
        product,
        curcode,
        DATE '{RDATE}' as reptdate
    FROM read_parquet('{CURRENT_FILE}')
    
    UNION ALL
    
    SELECT 
        branch,
        acctno,
        curbal,
        forbal,
        openind,
        product,
        curcode,
        DATE '{RDATE}' as reptdate
    FROM read_parquet('{VOSTRO_FILE}')
""")

print(f"CURCA records created: {con.execute('SELECT COUNT(*) FROM curca').fetchone()[0]}")

# ============================================================================
# MACRO APPEND - Append or replace monthly data
# Logic: If day 1 of month, create new file; otherwise append to existing
# ============================================================================
print(f"\nProcessing monthly append logic for day {REPTDAY}...")

SA_FILE = MIS_DIR / f"SA{REPTMON}.parquet"
FD_FILE_OUT = MIS_DIR / f"FD{REPTMON}.parquet"
CA_FILE = MIS_DIR / f"CA{REPTMON}.parquet"

if REPTDAY == "01":
    # First day of month - create new monthly files
    print("Day 01 detected - Creating new monthly files")
    
    # MIS.SA{REPTMON}
    sa_data = con.execute("SELECT * FROM cursa").arrow()
    pq.write_table(sa_data, SA_FILE)
    
    # MIS.FD{REPTMON}
    fd_data = con.execute("SELECT * FROM curfd").arrow()
    pq.write_table(fd_data, FD_FILE_OUT)
    
    # MIS.CA{REPTMON}
    ca_data = con.execute("SELECT * FROM curca").arrow()
    pq.write_table(ca_data, CA_FILE)
    
else:
    # Not first day - append to existing monthly files after deleting current date records
    print(f"Day {REPTDAY} detected - Appending to existing monthly files")
    
    # Process SA
    if SA_FILE.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE mis_sa_existing AS
            SELECT *
            FROM read_parquet('{SA_FILE}')
            WHERE reptdate != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE mis_sa_combined AS
            SELECT * FROM cursa
            UNION ALL
            SELECT * FROM mis_sa_existing
        """)
        
        sa_data = con.execute("SELECT * FROM mis_sa_combined").arrow()
        pq.write_table(sa_data, SA_FILE)
    else:
        sa_data = con.execute("SELECT * FROM cursa").arrow()
        pq.write_table(sa_data, SA_FILE)
    
    # Process FD
    if FD_FILE_OUT.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE mis_fd_existing AS
            SELECT *
            FROM read_parquet('{FD_FILE_OUT}')
            WHERE reptdate != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE mis_fd_combined AS
            SELECT * FROM curfd
            UNION ALL
            SELECT * FROM mis_fd_existing
        """)
        
        fd_data = con.execute("SELECT * FROM mis_fd_combined").arrow()
        pq.write_table(fd_data, FD_FILE_OUT)
    else:
        fd_data = con.execute("SELECT * FROM curfd").arrow()
        pq.write_table(fd_data, FD_FILE_OUT)
    
    # Process CA
    if CA_FILE.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE mis_ca_existing AS
            SELECT *
            FROM read_parquet('{CA_FILE}')
            WHERE reptdate != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE mis_ca_combined AS
            SELECT * FROM curca
            UNION ALL
            SELECT * FROM mis_ca_existing
        """)
        
        ca_data = con.execute("SELECT * FROM mis_ca_combined").arrow()
        pq.write_table(ca_data, CA_FILE)
    else:
        ca_data = con.execute("SELECT * FROM curca").arrow()
        pq.write_table(ca_data, CA_FILE)

print(f"Monthly files updated: SA{REPTMON}, FD{REPTMON}, CA{REPTMON}")

# ============================================================================
# PROC SORT and PROC SUMMARY for SA - Calculate MTD average balances
# ============================================================================
print(f"\nCalculating Saving/UMA average balances...")

con.execute(f"""
    CREATE OR REPLACE TABLE cursa_sorted AS
    SELECT *
    FROM read_parquet('{SA_FILE}')
    ORDER BY acctno
""")

con.execute(f"""
    CREATE OR REPLACE TABLE savg_summary AS
    SELECT 
        acctno,
        SUM(curbal) as mtdavbal_mis
    FROM cursa_sorted
    WHERE reptdate <= DATE '{RDATE}'
    GROUP BY acctno
    ORDER BY acctno
""")

# Calculate average by dividing by number of days
con.execute(f"""
    CREATE OR REPLACE TABLE mis_savg AS
    SELECT 
        acctno,
        mtdavbal_mis / {int(REPTDAY)} as mtdavbal_mis
    FROM savg_summary
""")

# Save MIS.SAVG{REPTMON}
savg_data = con.execute("SELECT * FROM mis_savg").arrow()
savg_file = MIS_DIR / f"SAVG{REPTMON}.parquet"
pq.write_table(savg_data, savg_file)
csv.write_csv(savg_data, MIS_DIR / f"SAVG{REPTMON}.csv")
print(f"Saved: MIS.SAVG{REPTMON} ({len(savg_data)} accounts)")

# ============================================================================
# PROC SORT and PROC SUMMARY for FD - Calculate MTD average balances
# ============================================================================
print(f"Calculating FD average balances...")

con.execute(f"""
    CREATE OR REPLACE TABLE curfd_sorted AS
    SELECT *
    FROM read_parquet('{FD_FILE_OUT}')
    ORDER BY acctno
""")

con.execute(f"""
    CREATE OR REPLACE TABLE favg_summary AS
    SELECT 
        acctno,
        SUM(curbal) as mtdavbal_mis,
        SUM(forbal) as mtdavforbal_mis
    FROM curfd_sorted
    WHERE reptdate <= DATE '{RDATE}'
    GROUP BY acctno
    ORDER BY acctno
""")

# Calculate average by dividing by number of days
con.execute(f"""
    CREATE OR REPLACE TABLE mis_favg AS
    SELECT 
        acctno,
        mtdavbal_mis / {int(REPTDAY)} as mtdavbal_mis,
        mtdavforbal_mis / {int(REPTDAY)} as mtdavforbal_mis
    FROM favg_summary
""")

# Save MIS.FAVG{REPTMON}
favg_data = con.execute("SELECT * FROM mis_favg").arrow()
favg_file = MIS_DIR / f"FAVG{REPTMON}.parquet"
pq.write_table(favg_data, favg_file)
csv.write_csv(favg_data, MIS_DIR / f"FAVG{REPTMON}.csv")
print(f"Saved: MIS.FAVG{REPTMON} ({len(favg_data)} accounts)")

# ============================================================================
# PROC SORT and PROC SUMMARY for CA - Calculate MTD average balances
# ============================================================================
print(f"Calculating Current/Vostro average balances...")

con.execute(f"""
    CREATE OR REPLACE TABLE curca_sorted AS
    SELECT *
    FROM read_parquet('{CA_FILE}')
    ORDER BY acctno
""")

con.execute(f"""
    CREATE OR REPLACE TABLE cavg_summary AS
    SELECT 
        acctno,
        SUM(curbal) as mtdavbal_mis,
        SUM(forbal) as mtdavforbal_mis
    FROM curca_sorted
    WHERE reptdate <= DATE '{RDATE}'
    GROUP BY acctno
    ORDER BY acctno
""")

# Calculate average by dividing by number of days
con.execute(f"""
    CREATE OR REPLACE TABLE mis_cavg AS
    SELECT 
        acctno,
        mtdavbal_mis / {int(REPTDAY)} as mtdavbal_mis,
        mtdavforbal_mis / {int(REPTDAY)} as mtdavforbal_mis
    FROM cavg_summary
""")

# Save MIS.CAVG{REPTMON}
cavg_data = con.execute("SELECT * FROM mis_cavg").arrow()
cavg_file = MIS_DIR / f"CAVG{REPTMON}.parquet"
pq.write_table(cavg_data, cavg_file)
csv.write_csv(cavg_data, MIS_DIR / f"CAVG{REPTMON}.csv")
print(f"Saved: MIS.CAVG{REPTMON} ({len(cavg_data)} accounts)")

# ============================================================================
# Summary Report
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"Report Date: {RDATE}")
print(f"Report Month: {REPTMON}")
print(f"Report Day: {REPTDAY}")
print(f"Days in calculation: {REPTDAY}")
print("="*80)
print(f"{'Dataset':<20} {'Accounts':<15} {'Output File':<30}")
print("-"*80)
print(f"{'Saving/UMA (SA)':<20} {len(savg_data):<15} {'SAVG{}.parquet'.format(REPTMON):<30}")
print(f"{'Fixed Deposit (FD)':<20} {len(favg_data):<15} {'FAVG{}.parquet'.format(REPTMON):<30}")
print(f"{'Current/Vostro (CA)':<20} {len(cavg_data):<15} {'CAVG{}.parquet'.format(REPTMON):<30}")
print("="*80)

print("\nMonthly accumulation files:")
print(f"  - MIS.SA{REPTMON}.parquet (daily records)")
print(f"  - MIS.FD{REPTMON}.parquet (daily records)")
print(f"  - MIS.CA{REPTMON}.parquet (daily records)")

print("\nAverage balance files:")
print(f"  - MIS.SAVG{REPTMON}.parquet (account averages)")
print(f"  - MIS.FAVG{REPTMON}.parquet (account averages)")
print(f"  - MIS.CAVG{REPTMON}.parquet (account averages)")

# Close connection
con.close()
print("\nProcessing complete!")
