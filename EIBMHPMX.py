import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input paths
ELDS_REPTDATE = INPUT_DIR / 'elds' / 'reptdate.parquet'
ELDS_ELN = INPUT_DIR / 'elds' / 'eln{reptmon}{nowk}{reptyear}.parquet'

# Output paths
HP01_REPORT = OUTPUT_DIR / 'hp01_report.txt'
HP02_REPORT = OUTPUT_DIR / 'hp02_report.txt'
HP03_EXCEPT = OUTPUT_DIR / 'hp03_exception.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize DuckDB and setup formats (PBBLNFMT)
# ============================================================================
con = duckdb.connect()

# HPD product list (from PBBLNFMT)
HPD_PRODUCTS = [700, 705, 710, 715, 720, 725, 730, 735, 740, 745]

# ============================================================================
# Get REPTDATE
# ============================================================================
reptdate_df = pl.read_parquet(ELDS_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
if day == 8: wk, wk1 = '1', '4'
elif day == 15: wk, wk1 = '2', '1'
elif day == 22: wk, wk1 = '3', '2'
else: wk, wk1 = '4', '3'

reptmon = f"{reptdate.month:02d}"
reptyear = f"{reptdate.year % 100:02d}"
rdate = reptdate.strftime('%d%m%y')

print(f"HP MAX/MIN Report - {reptmon}/{reptyear}")

# ============================================================================
# Load and process HP data from all weeks
# ============================================================================
hp_data = []
for nowk in ['1', '2', '3', '4']:
    eln_path = str(ELDS_ELN).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)
    if Path(eln_path).exists():
        hp_data.append(f"SELECT * FROM read_parquet('{eln_path}')")

union_query = " UNION ALL ".join(hp_data) if hp_data else "SELECT NULL"

con.execute(f"""
    CREATE TEMP TABLE hp_base AS
    SELECT *,
        amount as amtrel,
        CASE WHEN pricing > 0 THEN amount * (pricing / 100.0) ELSE 0 END as aprbal,
        pcodfiss as fisscode,
        LPAD(CAST(sector AS VARCHAR), 4, '0') as sectorcd
    FROM ({union_query})
    WHERE UPPER(status) = 'APPROVED'
      AND product IN ({','.join(map(str, HPD_PRODUCTS))})
      AND product NOT IN (380, 381)
""")

# Apply sector grouping logic
con.execute("""
    CREATE TEMP TABLE hp AS
    SELECT *,
        CASE 
            WHEN SUBSTRING(sectorcd, 1, 2) IN ('11','12','13','14') THEN '1000'
            WHEN SUBSTRING(sectorcd, 1, 2) IN ('21','22','23','24','29') THEN '2000'
            WHEN SUBSTRING(sectorcd, 1, 1) = '3' THEN '3000'
            WHEN SUBSTRING(sectorcd, 1, 1) = '4' THEN '4000'
            WHEN SUBSTRING(sectorcd, 1, 1) = '5' THEN '5000'
            WHEN SUBSTRING(sectorcd, 1, 1) = '6' THEN
                CASE WHEN SUBSTRING(sectorcd, 1, 2) IN ('61','62') THEN '6100' ELSE '6300' END
            WHEN SUBSTRING(sectorcd, 1, 1) = '7' THEN '7000'
            WHEN SUBSTRING(sectorcd, 1, 1) = '8' THEN '8000'
            WHEN SUBSTRING(sectorcd, 1, 2) IN ('91','92','93','94','95','96') THEN '9000'
            ELSE '0009'
        END as sectcd,
        CASE WHEN pcodfiss IS NULL THEN '0009' ELSE CAST(pcodfiss AS VARCHAR) END as fisscode_adj
    FROM hp_base
    ORDER BY aano, status
""")

con.execute("CREATE TEMP TABLE hp_dedup AS SELECT DISTINCT ON (aano, status) * FROM hp")

# Conventional HP only
con.execute("""
    CREATE TEMP TABLE hpconv AS
    SELECT * FROM hp_dedup
    WHERE product IN (700, 705, 720, 725)
""")

# ============================================================================
# MAXMIN Macro Function
# ============================================================================
def generate_maxmin_report(field_name, filter_cond, title, output_file):
    """Generate max/min rate report for given field and filter"""
    
    # Apply filter
    con.execute(f"""
        CREATE TEMP TABLE hp01 AS
        SELECT *, {field_name} as field1
        FROM hpconv
        WHERE {filter_cond}
    """)
    
    # Calculate higher and lower rates
    con.execute("""
        CREATE TEMP TABLE hprate AS
        SELECT field1, pricing, SUM(amtrel) as amtrel
        FROM hp01
        WHERE pricing > 0
        GROUP BY field1, pricing
        ORDER BY field1, pricing, amtrel
    """)
    
    # Get min and max rates per field
    con.execute("""
        CREATE TEMP TABLE rate_bounds AS
        SELECT field1,
            FIRST_VALUE(pricing) OVER (PARTITION BY field1 ORDER BY pricing, amtrel) as lowrate,
            FIRST_VALUE(amtrel) OVER (PARTITION BY field1 ORDER BY pricing, amtrel) as lownetp,
            FIRST_VALUE(pricing) OVER (PARTITION BY field1 ORDER BY pricing DESC, amtrel DESC) as hirate,
            FIRST_VALUE(amtrel) OVER (PARTITION BY field1 ORDER BY pricing DESC, amtrel DESC) as hinetp
        FROM hprate
    """)
    
    con.execute("CREATE TEMP TABLE ratelow AS SELECT DISTINCT field1, lowrate, lownetp FROM rate_bounds")
    con.execute("CREATE TEMP TABLE ratehigh AS SELECT DISTINCT field1, hirate, hinetp FROM rate_bounds")
    
    # Calculate weighted rate & balance
    con.execute("""
        CREATE TEMP TABLE hprel AS
        SELECT field1,
            COUNT(*) as noacct,
            SUM(aprbal) as aprbal,
            SUM(amtrel) as amtrel,
            (SUM(aprbal) / NULLIF(SUM(amtrel), 0)) * 100 as weigrate
        FROM hp01
        GROUP BY field1
    """)
    
    con.execute("""
        CREATE TEMP TABLE hprel_calc AS
        SELECT *,
            amtrel * (weigrate / 100) as weigbal
        FROM hprel
    """)
    
    # Merge all data
    con.execute("""
        CREATE TEMP TABLE hprel_final AS
        SELECT h.*, l.lowrate, l.lownetp, r.hirate, r.hinetp
        FROM hprel_calc h
        LEFT JOIN ratelow l ON h.field1 = l.field1
        LEFT JOIN ratehigh r ON h.field1 = r.field1
        ORDER BY h.field1
    """)
    
    # Generate report
    results = con.execute("""
        SELECT field1, weigrate, amtrel, weigbal, lowrate, lownetp, hirate, hinetp, aprbal
        FROM hprel_final
        ORDER BY field1
    """).fetchall()
    
    # Calculate totals
    totals = con.execute("""
        SELECT 
            SUM(aprbal) as total_aprbal,
            SUM(amtrel) as total_amtrel,
            (SUM(aprbal) / NULLIF(SUM(amtrel), 0)) * 100 as total_weigrate
        FROM hprel_final
    """).fetchone()
    
    total_weigbal = totals[1] * (totals[2] / 100) if totals[2] else 0
    
    # Write report
    with open(output_file, 'w') as f:
        f.write('PUBLIC BANK BERHAD\n')
        f.write(f'HP(ELDS) ON NEW LOANS APPROVED FOR {reptmon}/{reptyear}\n')
        f.write(f'{title}\n\n')
        f.write(f'{"DESC":<12} {"AVERAGE LENDING":<18} {"AMOUNT":<22} {"WEIGHTED":<22} {"MIN. LENDING":<18} {"AMOUNT":<22} {"MAX. LENDING":<18} {"AMOUNT":<22}\n')
        f.write(f'{"":<12} {"RATE(%)":<18} {"APPROVED":<22} {"BALANCE":<22} {"RATE(%)":<18} {"APPROVED":<22} {"RATE(%)":<18} {"APPROVED":<22}\n')
        f.write('=' * 180 + '\n')
        
        for row in results:
            f.write(f'{row[0]:<12} {row[1]:>18.2f} {row[2]:>22,.2f} {row[3]:>22,.2f} ')
            f.write(f'{row[4] if row[4] else 0:>18.2f} {row[5] if row[5] else 0:>22,.2f} ')
            f.write(f'{row[6] if row[6] else 0:>18.2f} {row[7] if row[7] else 0:>22,.2f}\n')
        
        f.write('-' * 180 + '\n')
        f.write(f'{"TOT":<12} {totals[2]:>18.2f} {totals[1]:>22,.2f} {total_weigbal:>22,.2f}\n')
        f.write('=' * 180 + '\n\n')

# ============================================================================
# Generate HP Reports
# ============================================================================
# Report 1: Non-individuals by sector
generate_maxmin_report(
    'sectcd',
    'custcode NOT IN (77, 78, 95, 96)',
    'NON INDIVIDUALS BY BUSINESS RELATED SECTORS',
    HP01_REPORT
)

# Report 2: Individuals by purpose
with open(HP01_REPORT, 'a') as f:
    f.write('\n\n')

generate_maxmin_report(
    'fisscode_adj',
    'custcode IN (77, 78, 95, 96)',
    'INDIVIDUALS BY PURPOSE (PCODFISS)',
    HP01_REPORT
)

# ============================================================================
# SME Report
# ============================================================================
con.execute("""
    CREATE TEMP TABLE hpsme AS
    SELECT *,
        1 as noacc,
        CASE WHEN smesize IN (41, 42, 43) THEN 1 ELSE 0 END as bnoacc,
        CASE WHEN smesize IN (41, 42, 43) THEN amtrel ELSE 0 END as bamtrel,
        CASE WHEN branch > 3000 THEN branch - 3000 ELSE branch END as branch_adj
    FROM hp_dedup
    WHERE LENGTH(newic) < 12
      AND smesize IN (41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54)
""")

con.execute("""
    CREATE TEMP TABLE hpsum AS
    SELECT branch_adj as branch,
        SUM(noacc) as noacc,
        SUM(amtrel) as amtrel,
        SUM(bnoacc) as bnoacc,
        SUM(bamtrel) as bamtrel
    FROM hpsme
    GROUP BY branch_adj
    ORDER BY branch_adj
""")

sme_results = con.execute("SELECT * FROM hpsum").fetchall()
sme_totals = con.execute("""
    SELECT SUM(noacc), SUM(amtrel), SUM(bnoacc), SUM(bamtrel) FROM hpsum
""").fetchone()

with open(HP02_REPORT, 'w') as f:
    f.write(f'APPROVED OF HP(CONV & ISLM) FOR SME CUSTOMER FOR THE MONTH {reptmon}/{reptyear}\n\n')
    f.write(f'{"BRANCH":<10} {"NO OF A/C":<15} {"AMOUNT APPROVED":<25} {"NO OF A/C":<15} {"AMOUNT APPROVED":<25}\n')
    f.write(f'{"":<10} {"":<15} {"FOR SME (RM)":<25} {"":<15} {"FOR BUMI SME (RM)":<25}\n')
    f.write('-' * 100 + '\n')
    
    for row in sme_results:
        f.write(f'{row[0]:<10} {row[1]:>15,} {row[2]:>25,.2f} {row[3]:>15,} {row[4]:>25,.2f}\n')
    
    f.write('=' * 100 + '\n')
    f.write(f'{"TOTAL":<10} {sme_totals[0]:>15,} {sme_totals[1]:>25,.2f} {sme_totals[2]:>15,} {sme_totals[3]:>25,.2f}\n')

# ============================================================================
# Exception Report
# ============================================================================
con.execute("""
    CREATE TEMP TABLE except AS
    SELECT *
    FROM hp_dedup
    WHERE (custcode IS NULL OR sector IS NULL OR pcodfiss IS NULL)
       OR (custcode IN (77, 78, 95, 96) AND pcodfiss > 1000)
       OR (custcode NOT IN (77, 78, 95, 96) AND sector < 1000)
""")

except_results = con.execute("""
    SELECT aano, name, newic, branch, custcode, sector, product, pricing, 
           amount, prodesc, pcodfiss, status, smesize, aadate
    FROM except
""").fetchall()

with open(HP03_EXCEPT, 'w') as f:
    f.write(f'EXCEPTION REPORT FOR APPROVED AA FOR THE MONTH {reptmon}/{reptyear}\n')
    f.write('AANO;NAME;NEWIC;BRANCH;CUSTCODE;SECTOR;PRODUCT;PRICING;AMOUNT;PRODESC;PCODFISS;STATUS;SMESIZE;AADATE\n')
    
    for row in except_results:
        aadate = row[13].strftime('%d%b%y').upper() if row[13] else ''
        f.write(f'{row[0]};{row[1]};{row[2]};{row[3]};{row[4]};{row[5]};{row[6]};{row[7]};{row[8]};{row[9]};{row[10]};{row[11]};{row[12]};{aadate}\n')

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {HP01_REPORT}")
print(f"  - {HP02_REPORT}")
print(f"  - {HP03_EXCEPT}")
