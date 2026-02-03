import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Format mappings (PBBDPFMT, PBBELF)
BRCHCD = {}      # Branch code to abbreviation
DDCUSTCD = {}    # Customer code format

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/mnitb/reptdate.parquet')").fetchone()[0]
reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{reptdate.day:02d}"
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Report Date: {rdate}")

# ============================================================================
# PROCESS CURRENT ACCOUNT MOVEMENTS (CAMV)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE camv AS
    SELECT *, 
           branch brabv,  -- Apply BRCHCD format if available
           CASE WHEN custcode IN (2,3,7,10,12,81,82,83,84) THEN 1 ELSE 0 END exclude,
           CASE WHEN product BETWEEN 400 AND 411 OR product BETWEEN 420 AND 431 OR product BETWEEN 432 AND 434
                THEN 1 ELSE 0 END fy_product,
           CASE WHEN custcode IN (77,78,95,96) THEN 'I' ELSE 'C' END cust_type,
           CASE WHEN costctr BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END bank_type
    FROM read_parquet('{INPUT_DIR}/omy/camv{reptday}{reptmon}.parquet')
    WHERE product NOT IN (79,80,413)
      AND custcode NOT IN (2,3,7,10,12,81,82,83,84)
""")

# Split into 6 categories
categories = {
    'CAMFYI': ('fy_product=1 AND curcode!=\'MYR\' AND cust_type=\'I\'', 
               'FOREIGN CURRENCY) INDIVIDUAL', 'NETBALC'),
    'CAMFYC': ('fy_product=1 AND curcode!=\'MYR\' AND cust_type=\'C\'', 
               'FOREIGN CURRENCY) CORPORATE', 'NETBALC'),
    'CAMII':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'I\'', 
               'INDIVIDUAL CUSTOMERS - ISLAMIC)', 'NETBALC'),
    'CAMIC':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'C\'', 
               'INDIVIDUAL CUSTOMERS-CONVENTIONAL)', 'NETBALC'),
    'CAMCI':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'I\' AND acctno NOT BETWEEN 3790000000 AND 3799999999', 
               'CORPORATE CUSTOMERS - ISLAMIC)', 'NETBALC'),
    'CAMCC':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'C\' AND acctno NOT BETWEEN 3590000000 AND 3599999999', 
               'CORPORATE CUSTOMERS - CONVENTIONAL)', 'NETBALC')
}

for fname, (cond, desc, netcol) in categories.items():
    data = con.execute(f"SELECT * FROM camv WHERE {cond}").fetchall()
    
    with open(OUTPUT_DIR/f'{fname.lower()}.csv', 'w') as f:
        f.write(f"CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT BY BRANCH ({desc}\n")
        f.write(f"AS AT {rdate}\n")
        f.write("BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE\n")
        
        tcurbal = tpcurbal = tnetbal = 0.0
        for r in data:
            f.write(f"{r[1]};{r[-4]};{r[2]};{r[0]};{r[3]};{r[4]};{r[5]};{r[6]}\n")
            tcurbal += r[3]
            tpcurbal += r[4]
            if netcol == 'NETBALC':
                tnetbal += r[5]
        
        if netcol == 'NETBALC':
            f.write(f";;;;{tcurbal};{tpcurbal};{tnetbal}\n")
        else:
            f.write(f";;;;{tcurbal};{tpcurbal}\n")
    
    print(f"{fname}: {len(data)} records")

# ============================================================================
# PROCESS FIXED DEPOSIT MOVEMENTS (FDMV)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE fdmv AS
    SELECT *, 
           branch brabv,
           CASE WHEN product BETWEEN 350 AND 362 THEN 1 ELSE 0 END fy_product,
           CASE WHEN custcode IN (77,78,95,96) THEN 'I' ELSE 'C' END cust_type,
           CASE WHEN costctr BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END bank_type
    FROM read_parquet('{INPUT_DIR}/omy/fdmv{reptday}{reptmon}.parquet')
""")

fd_categories = {
    'FDMFYI': ('fy_product=1 AND cust_type=\'I\'', 
               'FOREIGN CURRENCY) INDIVIDUAL'),
    'FDMFYC': ('fy_product=1 AND cust_type=\'C\'', 
               'FOREIGN CURRENCY) CORPORATE'),
    'FDMII':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'I\'', 
               'INDIVIDUAL CUSTOMERS - ISLAMIC)'),
    'FDMIC':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'C\'', 
               'INDIVIDUAL CUSTOMERS - CONVENTIONAL)'),
    'FDMCI':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'I\'', 
               'CORPORATE CUSTOMERS - ISLAMIC)'),
    'FDMCC':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'C\'', 
               'CORPORATE CUSTOMERS - CONVENTIONAL)')
}

for fname, (cond, desc) in fd_categories.items():
    data = con.execute(f"SELECT * FROM fdmv WHERE {cond}").fetchall()
    
    title = 'FIXED DEPOSIT' if fname.startswith('FDMFY') else 'FD ACCOUNT'
    
    with open(OUTPUT_DIR/f'{fname.lower()}.csv', 'w') as f:
        f.write(f"{title} MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT BY BRANCH ({desc}\n")
        f.write(f"AS AT {rdate}\n")
        f.write("BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE\n")
        
        tcurbal = tpcurbal = 0.0
        for r in data:
            f.write(f"{r[1]};{r[-3]};{r[2]};{r[0]};{r[3]};{r[4]};{r[5]};{r[6]}\n")
            tcurbal += r[3]
            tpcurbal += r[4]
        
        f.write(f";;;;{tcurbal};{tpcurbal}\n")
    
    print(f"{fname}: {len(data)} records")

con.close()
print(f"\nCompleted: 12 CSV reports generated in {OUTPUT_DIR}")
