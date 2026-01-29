import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input text files (fixed-width)
ELDSTX1 = INPUT_DIR / 'eldstx1.txt'
ELDSTX2 = INPUT_DIR / 'eldstx2.txt'
ELDSTX3 = INPUT_DIR / 'eldstx3.txt'
ELDSTX4 = INPUT_DIR / 'eldstx4.txt'
ELDSTX5 = INPUT_DIR / 'eldstx5.txt'
ELDSTX7 = INPUT_DIR / 'eldstx7.txt'
ELDSTX10 = INPUT_DIR / 'eldstx10.txt'
ELDSTX17 = INPUT_DIR / 'eldstx17.txt'
ELDSHP1 = INPUT_DIR / 'eldshp1.txt'
ELDSHP2 = INPUT_DIR / 'eldshp2.txt'
ELDSHP3 = INPUT_DIR / 'eldshp3.txt'
ELDSHP4 = INPUT_DIR / 'eldshp4.txt'
ELDSHP5 = INPUT_DIR / 'eldshp5.txt'
ELDSHP10 = INPUT_DIR / 'eldshp10.txt'
ELDSHP17 = INPUT_DIR / 'eldshp17.txt'

# Output paths
ELDS_REPTDATE = OUTPUT_DIR / 'elds' / 'reptdate.parquet'
ELDS_ELN = OUTPUT_DIR / 'elds' / 'eln.parquet'
ELDS1_EHPLNMAX = OUTPUT_DIR / 'elds1' / 'ehplnmax{reptmon}{nowk}{reptyear}.parquet'
ELDS1_MELNA_47 = OUTPUT_DIR / 'elds1' / 'melna_47{reptmon}{nowk}{reptyear}.parquet'
ELDS1_MEHPA_4 = OUTPUT_DIR / 'elds1' / 'mehpa_4{reptmon}{nowk}{reptyear}.parquet'
ELDS1_EHPLNEND = OUTPUT_DIR / 'elds1' / 'ehplnend{reptmon}{nowk}{reptyear}.parquet'

(OUTPUT_DIR / 'elds').mkdir(parents=True, exist_ok=True)
(OUTPUT_DIR / 'elds1').mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize DuckDB
# ============================================================================
con = duckdb.connect()

# ============================================================================
# Calculate REPTDATE based on current day
# ============================================================================
today = datetime.now().date()
day = today.day

if 8 <= day <= 14:
    reptdate = datetime(today.year, today.month, 8).date()
    wk = '1'
elif 15 <= day <= 21:
    reptdate = datetime(today.year, today.month, 15).date()
    wk = '2'
elif 22 <= day <= 27:
    reptdate = datetime(today.year, today.month, 22).date()
    wk = '3'
else:  # Last day of previous month
    first_of_month = today.replace(day=1)
    reptdate = first_of_month - timedelta(days=1)
    wk = '4'

nowk = wk
rdate = reptdate.strftime('%d%m%y')
reptmon = f"{reptdate.month:02d}"
reptyear = f"{reptdate.year % 100:02d}"

print(f"ELDS Processing - {reptdate.strftime('%d%b%Y')}")
print(f"Week: {nowk}, Month: {reptmon}, Year: {reptyear}")

# Save REPTDATE
reptdate_df = pl.DataFrame({'reptdate': [reptdate]})
reptdate_df.write_parquet(ELDS_REPTDATE)

# ============================================================================
# Read fixed-width files - ELN1-3 (Loan files)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln1 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 17, 60)) as name,
        UPPER(SUBSTRING(line, 983, 30)) as status
    FROM read_csv('{ELDSTX1}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

con.execute(f"""
    CREATE TEMP TABLE eln2 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 993, 30)) as status
    FROM read_csv('{ELDSTX2}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

con.execute(f"""
    CREATE TEMP TABLE eln3 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 17, 13)) as maano,
        UPPER(SUBSTRING(line, 781, 2)) as collcode,
        UPPER(SUBSTRING(line, 786, 30)) as status
    FROM read_csv('{ELDSTX3}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

# ============================================================================
# Read fixed-width files - EHP1-3 (HP files)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE ehp1 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 17, 60)) as name,
        UPPER(SUBSTRING(line, 983, 30)) as status,
        COALESCE(NULLIF(UPPER(SUBSTRING(line, 1474, 8)), ''), 'EHPDS') as sys_type
    FROM read_csv('{ELDSHP1}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

con.execute(f"""
    CREATE TEMP TABLE ehp2 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 993, 30)) as status
    FROM read_csv('{ELDSHP2}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

con.execute(f"""
    CREATE TEMP TABLE ehp3 AS
    SELECT DISTINCT
        UPPER(SUBSTRING(line, 1, 13)) as aano,
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 781, 2)) as collcode,
        UPPER(SUBSTRING(line, 786, 30)) as status
    FROM read_csv('{ELDSHP3}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY aano, status
""")

# ============================================================================
# Merge ELN and EHP files
# ============================================================================
con.execute("""
    CREATE TEMP TABLE eln_merged AS
    SELECT e1.*, e2.status as status2, e3.maano, e3.collcode
    FROM eln1 e1
    LEFT JOIN eln2 e2 ON e1.aano = e2.aano AND e1.status = e2.status
    LEFT JOIN eln3 e3 ON e1.aano = e3.aano AND e1.status = e3.status
""")

con.execute("""
    CREATE TEMP TABLE ehp_merged AS
    SELECT e1.*, e2.status as status2, e3.maano, e3.collcode
    FROM ehp1 e1
    LEFT JOIN ehp2 e2 ON e1.aano = e2.aano AND e1.status = e2.status
    LEFT JOIN ehp3 e3 ON e1.aano = e3.aano AND e1.status = e3.status
""")

con.execute("""
    CREATE TABLE eln_final AS
    SELECT aano, name, status, maano, collcode
    FROM eln_merged
    UNION ALL
    SELECT aano, name, status, maano, collcode
    FROM ehp_merged
    WHERE SUBSTRING(maano, 2, 5) != '*****'
""")

con.execute(f"COPY (SELECT DISTINCT * FROM eln_final ORDER BY aano, status) TO '{ELDS_ELN}'")

# ============================================================================
# Read additional ELN/EHP files (A4, A5, A7)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE elna4 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 17, 50)) as name,
        UPPER(SUBSTRING(line, 70, 10)) as apvby,
        UPPER(SUBSTRING(line, 196, 40)) as job,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 306, 15), ',', ''), ' ', '') AS DECIMAL(15,2)) as salary,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 342, 15), ',', ''), ' ', '') AS DECIMAL(15,2)) as expend1,
        UPPER(SUBSTRING(line, 710, 20)) as gainemp
    FROM read_csv('{ELDSTX4}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE ehpa4 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 17, 50)) as name,
        UPPER(SUBSTRING(line, 70, 10)) as apvby,
        UPPER(SUBSTRING(line, 196, 40)) as job,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 306, 15), ',', ''), ' ', '') AS DECIMAL(15,2)) as salary,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 342, 15), ',', ''), ' ', '') AS DECIMAL(15,2)) as expend1,
        UPPER(SUBSTRING(line, 710, 20)) as gainemp
    FROM read_csv('{ELDSHP4}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE elna5 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 93, 30)) as advmarg,
        UPPER(SUBSTRING(line, 126, 50)) as yrgdcbor,
        UPPER(SUBSTRING(line, 179, 50)) as yrgdcrbo
    FROM read_csv('{ELDSTX5}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE ehpa5 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 93, 30)) as advmarg,
        UPPER(SUBSTRING(line, 126, 50)) as yrgdcbor,
        UPPER(SUBSTRING(line, 179, 50)) as yrgdcrbo
    FROM read_csv('{ELDSHP5}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE elna7 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 44, 45)) as propert,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 225, 15), ',', ''), ' ', '') AS DECIMAL(15,2)) as cmvalue,
        UPPER(SUBSTRING(line, 543, 3)) as marcmv
    FROM read_csv('{ELDSTX7}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

# ============================================================================
# Merge to create EHPLNMAX
# ============================================================================
con.execute("""
    CREATE TEMP TABLE ehplnmax AS
    SELECT e.*, a5c.advmarg, a5c.yrgdcbor, a5c.yrgdcrbo,
           a5i.advmarg as advmarg_i, a5i.yrgdcbor as yrgdcbor_i, a5i.yrgdcrbo as yrgdcrbo_i
    FROM (SELECT DISTINCT maano, aano, name, collcode FROM eln_final WHERE maano IS NOT NULL) e
    LEFT JOIN elna5 a5c ON e.maano = a5c.maano
    LEFT JOIN ehpa5 a5i ON e.maano = a5i.maano
    WHERE SUBSTRING(e.maano, 2, 5) != '*****'
""")

con.execute(f"COPY ehplnmax TO '{str(ELDS1_EHPLNMAX).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'")

# ============================================================================
# Create MELNA_47 (A4 + A7)
# ============================================================================
con.execute("""
    CREATE TEMP TABLE melna_47 AS
    SELECT a4.*, a7.propert, a7.cmvalue, a7.marcmv
    FROM elna4 a4
    LEFT JOIN elna7 a7 ON a4.maano = a7.maano
    WHERE SUBSTRING(a4.maano, 2, 5) != '*****'
""")

con.execute(f"COPY melna_47 TO '{str(ELDS1_MELNA_47).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'")

# ============================================================================
# Create MEHPA_4
# ============================================================================
con.execute("""
    CREATE TEMP TABLE mehpa_4 AS
    SELECT * FROM ehpa4
    WHERE SUBSTRING(maano, 2, 5) != '*****'
""")

con.execute(f"COPY mehpa_4 TO '{str(ELDS1_MEHPA_4).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'")

# ============================================================================
# Read A10 and A17 files
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE elna10 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 112, 6)) as mrgadve,
        UPPER(SUBSTRING(line, 121, 1)) as ovrcrd1,
        UPPER(SUBSTRING(line, 125, 1)) as ovrcrd2,
        UPPER(SUBSTRING(line, 129, 1)) as ovrcrd3,
        UPPER(SUBSTRING(line, 133, 1)) as ovrcrd4,
        UPPER(SUBSTRING(line, 137, 1)) as ovrcrd5,
        UPPER(SUBSTRING(line, 141, 1)) as ovrcrd6,
        UPPER(SUBSTRING(line, 145, 1)) as ovrcrd7,
        UPPER(SUBSTRING(line, 149, 1)) as ovrcrd8,
        UPPER(SUBSTRING(line, 153, 1)) as ovrcrd9,
        UPPER(SUBSTRING(line, 157, 1)) as ovrcrd10,
        UPPER(SUBSTRING(line, 161, 1)) as ovrcrd11,
        UPPER(SUBSTRING(line, 165, 1)) as ovrcrd12,
        UPPER(SUBSTRING(line, 169, 1)) as ovrcrd13,
        UPPER(SUBSTRING(line, 173, 150)) as ovroth,
        UPPER(SUBSTRING(line, 326, 55)) as grpcode,
        UPPER(SUBSTRING(line, 736, 15)) as cmv
    FROM read_csv('{ELDSTX10}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE ehpa10 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        UPPER(SUBSTRING(line, 112, 6)) as mrgadve,
        UPPER(SUBSTRING(line, 121, 1)) as ovrcrd1,
        UPPER(SUBSTRING(line, 125, 1)) as ovrcrd2,
        UPPER(SUBSTRING(line, 129, 1)) as ovrcrd3,
        UPPER(SUBSTRING(line, 133, 1)) as ovrcrd4,
        UPPER(SUBSTRING(line, 137, 1)) as ovrcrd5,
        UPPER(SUBSTRING(line, 141, 1)) as ovrcrd6,
        UPPER(SUBSTRING(line, 145, 1)) as ovrcrd7,
        UPPER(SUBSTRING(line, 149, 1)) as ovrcrd8,
        UPPER(SUBSTRING(line, 153, 1)) as ovrcrd9,
        UPPER(SUBSTRING(line, 157, 1)) as ovrcrd10,
        UPPER(SUBSTRING(line, 161, 1)) as ovrcrd11,
        UPPER(SUBSTRING(line, 165, 1)) as ovrcrd12,
        UPPER(SUBSTRING(line, 169, 1)) as ovrcrd13,
        UPPER(SUBSTRING(line, 173, 150)) as ovroth,
        UPPER(SUBSTRING(line, 326, 55)) as grpcode,
        UPPER(SUBSTRING(line, 736, 15)) as cmv
    FROM read_csv('{ELDSHP10}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE elna17 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        CAST(SUBSTRING(line, 226, 3) AS INTEGER) as grtotscr,
        UPPER(SUBSTRING(line, 232, 50)) as bbrnge,
        CAST(SUBSTRING(line, 297, 3) AS INTEGER) as bbtotsc,
        UPPER(SUBSTRING(line, 323, 50)) as acrnge1,
        CAST(SUBSTRING(line, 388, 3) AS INTEGER) as actotsc1
    FROM read_csv('{ELDSTX17}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

con.execute(f"""
    CREATE TEMP TABLE ehpa17 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 13)) as maano,
        CAST(SUBSTRING(line, 226, 3) AS INTEGER) as grtotscr,
        UPPER(SUBSTRING(line, 232, 50)) as bbrnge,
        CAST(SUBSTRING(line, 297, 3) AS INTEGER) as bbtotsc,
        UPPER(SUBSTRING(line, 323, 50)) as acrnge1,
        CAST(SUBSTRING(line, 388, 3) AS INTEGER) as actotsc1
    FROM read_csv('{ELDSHP17}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY maano
""")

# ============================================================================
# Merge A10 and A17
# ============================================================================
con.execute("""
    CREATE TEMP TABLE ehplnend AS
    SELECT COALESCE(a10c.maano, a10i.maano, a17c.maano, a17i.maano) as maano,
           a10c.*, a10i.mrgadve as mrgadve_i,
           a17c.grtotscr, a17c.bbrnge, a17c.bbtotsc, a17c.acrnge1, a17c.actotsc1,
           a17i.grtotscr as grtotscr_i, a17i.bbrnge as bbrnge_i
    FROM elna10 a10c
    FULL OUTER JOIN ehpa10 a10i ON a10c.maano = a10i.maano
    FULL OUTER JOIN elna17 a17c ON COALESCE(a10c.maano, a10i.maano) = a17c.maano
    FULL OUTER JOIN ehpa17 a17i ON COALESCE(a10c.maano, a10i.maano) = a17i.maano
    WHERE SUBSTRING(COALESCE(a10c.maano, a10i.maano, a17c.maano, a17i.maano), 2, 5) != '*****'
""")

con.execute(f"COPY ehplnend TO '{str(ELDS1_EHPLNEND).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'")

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {ELDS_REPTDATE}")
print(f"  - {ELDS_ELN}")
print(f"  - {str(ELDS1_EHPLNMAX).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}")
print(f"  - {str(ELDS1_MELNA_47).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}")
print(f"  - {str(ELDS1_MEHPA_4).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}")
print(f"  - {str(ELDS1_EHPLNEND).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}")
