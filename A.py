import duckdb
from pathlib import Path
from datetime import timedelta

# Configuration
DP_PATH = Path("DP")
DPGF_PATH = Path("DPGF")
DPFILE_PATH = Path("path/to/dpfile.dat")
DPGF_PATH.mkdir(exist_ok=True)

# Initialize DuckDB connection
con = duckdb.connect()

# ===== STEP 1: REPTDATE Processing =====
reptdate = con.execute(f"""
    SELECT REPTDATE FROM '{DP_PATH / "REPTDATE.parquet"}'
""").fetchone()[0]

prevdate = reptdate - timedelta(days=60)

# Determine WK and WK1
day = reptdate.day
if 1 <= day <= 8:
    wk, wk1 = '1', '4'
elif 9 <= day <= 15:
    wk, wk1 = '2', '1'
elif 16 <= day <= 22:
    wk, wk1 = '3', '2'
else:
    wk, wk1 = '4', '3'

mm2 = reptdate.month - 1 if reptdate.month > 1 else 12

# Macro variables
NOWK = wk
REPTYEAR = str(reptdate.year)[-2:]
REPTMON = f"{reptdate.month:02d}"
PREVMON = f"{mm2:02d}"
REPTDAY = f"{reptdate.day:02d}"
RDATE = reptdate.strftime("%d/%m/%Y")

print(f"Report Date: {RDATE}")
print(f"Previous Date: {prevdate.strftime('%d/%m/%Y')}")

# ===== STEP 2: Read Fixed-Width Data File =====
def read_fixed_width_file(file_path, reptdate):
    """Parse fixed-width file per SAS INPUT spec"""
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            holdact = line[144]
            if holdact == '4':
                continue
            
            data.append({
                'REPTDATE': reptdate,
                'HOLDBRH': int(line[105:109]),
                'ACCTNO': int(line[109:115]),
                'SEQNUM': int(line[141:144]),
                'HOLDACT': holdact,
                'EARAMT': float(line[145:152]) / 100,
                'REASON': line[153:173].strip(),
                'DESC': line[173:193].strip(),
                'EARDATE': line[193:198],
                'EXPDATE': line[198:203],
                'USERID': line[214:222].strip()
            })
    return data

src_data = read_fixed_width_file(DPFILE_PATH, reptdate)
con.execute("CREATE TABLE src AS SELECT * FROM src_data")

# ===== STEP 3: Process CASA accounts using DuckDB SQL =====
con.execute(f"""
    CREATE TABLE casa AS
    SELECT ACCTNO, PRODUCT, BRANCH, USER3
    FROM (
        SELECT ACCTNO, PRODUCT, BRANCH, USER3
        FROM '{DP_PATH / "SAVING.parquet"}'
        WHERE OPENIND NOT IN ('B', 'C', 'P') 
          AND CUSTCODE IN (77, 78, 95, 96)
        
        UNION ALL
        
        SELECT ACCTNO, PRODUCT, BRANCH, USER3
        FROM '{DP_PATH / "CURRENT.parquet"}'
        WHERE OPENIND NOT IN ('B', 'C', 'P') 
          AND CUSTCODE IN (77, 78, 95, 96)
    )
""")

# ===== STEP 4: Merge SRC with CASA =====
con.execute("""
    CREATE TABLE src_merged AS
    SELECT s.*, c.PRODUCT, c.BRANCH, c.USER3
    FROM src s
    LEFT JOIN casa c ON s.ACCTNO = c.ACCTNO
""")

# ===== STEP 5: CHECKDS1 Logic using SQL =====
keep_earmark_path = DPGF_PATH / "KEEP_EARMARK.parquet"

if keep_earmark_path.exists():
    con.execute(f"""
        CREATE TABLE final_output AS
        SELECT * FROM (
            -- Existing data (filtered)
            SELECT *
            FROM '{keep_earmark_path}'
            WHERE REPTDATE != DATE '{reptdate}'
              AND REPTDATE >= DATE '{prevdate}'
            
            UNION ALL
            
            -- New data
            SELECT * FROM src_merged
        )
    """)
else:
    con.execute("CREATE TABLE final_output AS SELECT * FROM src_merged")

# ===== STEP 6: Write Output =====
con.execute(f"""
    COPY final_output 
    TO '{keep_earmark_path}' 
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")

# Summary statistics
result = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        MIN(REPTDATE) as min_date,
        MAX(REPTDATE) as max_date,
        COUNT(DISTINCT ACCTNO) as unique_accounts
    FROM final_output
""").fetchdf()

print(f"\nOutput written to: {keep_earmark_path}")
print("\nSummary Statistics:")
print(result)

con.close()
