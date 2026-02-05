#!/usr/bin/env python3
"""
EIIDARTB - Amanah Raya Trustees BHD Report
Generates detailed reports for ARTB trust fund accounts
"""

import duckdb
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/deposit/reptdate.parquet')").fetchone()[0]
day = reptdate.day
nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
reptyear, reptmon, reptday = reptdate.year, reptdate.month, day
tdate = reptdate.strftime('%d/%m/%Y')
rdate = int(reptdate.strftime('%Y%m%d'))

print(f"Amanah Raya Trustees Report - {tdate}")

ARTB_CUSTNOS = (3523050, 11335374, 11880426, 3728510, 13158067, 14368177,
                14368641, 14369065, 14387105, 14932947, 14960789, 15254645,
                15241330, 15310964, 15352797)

con.execute(f"""
    CREATE TEMP TABLE cisca AS
    SELECT 
        custno, acctno, custname,
        CASE WHEN newic != '' THEN newic ELSE oldic END icno,
        'CA' cistype
    FROM read_parquet('{INPUT_DIR}/cisdp/deposit.parquet')
    WHERE seccust = '901' AND acctno BETWEEN 3000000000 AND 3999999999
""")

con.execute(f"""
    CREATE TEMP TABLE ca AS
    SELECT c.*, ca.name, ca.curbal, ca.intpaybl, ca.intrate,
           curbal + COALESCE(intpaybl, 0) balance
    FROM cisca c
    JOIN read_parquet('{INPUT_DIR}/deposit/current.parquet') ca ON c.acctno = ca.acctno
    WHERE ca.curbal > 0
""")

con.execute(f"""
    CREATE TEMP TABLE cisfd AS
    SELECT 
        custno, acctno, custname,
        CASE WHEN newic != '' THEN newic ELSE oldic END icno,
        'FD' cistype
    FROM read_parquet('{INPUT_DIR}/cisfd/deposit.parquet')
    WHERE seccust = '901' 
      AND (acctno BETWEEN 1000000000 AND 1999999999
           OR acctno BETWEEN 4000000000 AND 6999999999
           OR acctno BETWEEN 7000000000 AND 7999999999)
""")

con.execute(f"""
    CREATE TEMP TABLE fd AS
    SELECT c.*, f.name, f.curbal, f.intpaybl, f.purpose, f.rate,
           f.depodte, f.matdate, f.term, f.matid, f.intpay,
           curbal + COALESCE(intpaybl, 0) balance
    FROM cisfd c
    JOIN read_parquet('{INPUT_DIR}/deposit/fd.parquet') f ON c.acctno = f.acctno
    WHERE f.curbal > 0 AND COALESCE(f.purpose, '') != '2'
""")

con.execute(f"""
    CREATE TEMP TABLE cafd AS
    SELECT 
        acctno, custno,
        CASE WHEN custname = '   ' THEN name ELSE custname END custname,
        cistype,
        CASE WHEN cistype = 'CA' THEN curbal ELSE 0 END cabal,
        CASE WHEN cistype = 'FD' THEN curbal ELSE 0 END fdbal,
        curbal, balance, intrate, rate, depodte, matdate, term, matid, intpay
    FROM (
        SELECT acctno, custno, custname, name, cistype, curbal, balance, 
               intrate, 0 rate, 0 depodte, 0 matdate, 0 term, '' matid, 0 intpay
        FROM ca
        UNION ALL
        SELECT acctno, custno, custname, name, cistype, curbal, balance,
               0 intrate, rate, depodte, matdate, term, matid, intpay
        FROM fd
    )
    WHERE custno IN {ARTB_CUSTNOS}
      AND curbal > 0
""")

con.execute(f"""
    CREATE TEMP TABLE depo AS
    SELECT acctno, fundname, REPLACE(fundname, ' ', '') fnamex
    FROM read_csv('{INPUT_DIR}/dpcust.txt', delim='|', header=false,
        columns={{'acctno': 'BIGINT', 'fundname': 'VARCHAR'}})
""")

pmfund_list = con.execute("SELECT DISTINCT fnamex FROM depo WHERE fundname LIKE '%PUBLIC%'").fetchall()
pmfund_set = {f[0] for f in pmfund_list}

con.execute("""
    CREATE TEMP TABLE cafd_final AS
    SELECT 
        c.*,
        COALESCE(d.fundname, c.custname) fundname,
        d.fnamex,
        CASE WHEN d.fnamex IS NOT NULL THEN 1 ELSE 2 END pmtyp
    FROM cafd c
    LEFT JOIN depo d ON c.acctno = d.acctno
""")

con.execute("""
    UPDATE cafd_final
    SET pmtyp = CASE WHEN fnamex IN (SELECT fnamex FROM depo WHERE fundname LIKE '%PUBLIC%') 
                     THEN 1 ELSE 2 END
    WHERE fnamex IS NOT NULL
""")

con.execute("ALTER TABLE cafd_final ADD COLUMN pmtype VARCHAR")
con.execute("UPDATE cafd_final SET pmtype = CASE WHEN pmtyp = 1 THEN 'PUBLIC MUTUAL FUND' ELSE 'NON-PUBLIC MUTUAL FUND' END")

con.execute("ALTER TABLE cafd_final ADD COLUMN fundmne VARCHAR")
con.execute("UPDATE cafd_final SET fundmne = SUBSTRING(fnamex, 1, 15)")

con.execute(f"COPY cafd_final TO '{OUTPUT_DIR}/cafd_artb.parquet'")

con.execute("""
    CREATE TEMP TABLE sumcafd AS
    SELECT pmtype, SUM(cabal) cabal, SUM(fdbal) fdbal, SUM(curbal) curbal
    FROM cafd_final
    GROUP BY pmtype
    ORDER BY pmtype
""")

con.execute("""
    CREATE TEMP TABLE totcafd AS
    SELECT 'TOTAL' pmtype, SUM(cabal) cabal, SUM(fdbal) fdbal, SUM(curbal) curbal
    FROM cafd_final
""")

with open(OUTPUT_DIR / 'artb_report.txt', 'w') as f:
    f.write("PUBLIC ISLAMIC BANK BERHAD\n")
    f.write(f"REPORT ID: EIIDARTB @ {tdate}\n\n")
    f.write("TABLE 1: SUMMARY TABLE FOR AMANAH RAYA GROUP\n")
    f.write("PUBLIC ISLAMIC BANK (PIBB)   CURRENT ACCOUNTS    FIXED DEPOSITS    TOTAL (RM)\n")
    
    for i, row in enumerate(con.execute("SELECT * FROM sumcafd").fetchall(), 1):
        pmtype, cabal, fdbal, curbal = row
        f.write(f"{i}) {pmtype:30} {cabal:18.2f} {fdbal:18.2f} {curbal:18.2f}\n")
    
    row = con.execute("SELECT * FROM totcafd").fetchone()
    f.write(f"{'TOTAL':33} {row[1]:18.2f} {row[2]:18.2f} {row[3]:18.2f}\n\n")

con.execute("""
    CREATE TEMP TABLE arca AS
    SELECT acctno, custno, custname, fundmne, fundname, pmtyp, curbal, balance, intrate
    FROM cafd_final
    WHERE cistype = 'CA'
    ORDER BY acctno
""")

con.execute("""
    CREATE TEMP TABLE arfd AS
    SELECT acctno, custno, custname, fundmne, fundname, pmtyp, curbal, balance, 
           rate, depodte, matdate, term, matid, intpay
    FROM cafd_final
    WHERE cistype = 'FD'
""")

con.execute("CREATE TEMP TABLE arca1 AS SELECT * FROM arca WHERE pmtyp = 1 ORDER BY acctno")
con.execute("CREATE TEMP TABLE arca2 AS SELECT * FROM arca WHERE pmtyp = 2 ORDER BY acctno")

con.execute("CREATE TEMP TABLE sumarca1 AS SELECT SUM(curbal) curbal, SUM(balance) balance FROM arca1")
con.execute("CREATE TEMP TABLE sumarca2 AS SELECT SUM(curbal) curbal, SUM(balance) balance FROM arca2")
con.execute("CREATE TEMP TABLE totarca AS SELECT SUM(curbal) curbal, SUM(balance) balance FROM arca")

con.execute(f"""
    ALTER TABLE arfd ADD COLUMN depdte VARCHAR
""")
con.execute(f"""
    ALTER TABLE arfd ADD COLUMN matdte VARCHAR
""")
con.execute(f"""
    ALTER TABLE arfd ADD COLUMN rmaindt INTEGER
""")

arfd_records = con.execute("SELECT * FROM arfd").fetchall()
updated_records = []

for row in arfd_records:
    acctno, custno, custname, fundmne, fundname, pmtyp, curbal, balance, rate, depodte, matdate, term, matid, intpay = row
    
    try:
        depo_dt = datetime.strptime(str(depodte)[:8], '%m%d%Y')
        depdte = depo_dt.strftime('%d/%m/%Y')
    except:
        depdte = ''
    
    try:
        mat_dt = datetime.strptime(str(matdate)[:8], '%Y%m%d')
        matdte = mat_dt.strftime('%d/%m/%Y')
        rmaindt = (mat_dt - reptdate).days
    except:
        matdte = ''
        rmaindt = 0
    
    matid = matid if matid and matid != '' else 'M'
    updated_records.append((acctno, custno, custname, fundmne, fundname, pmtyp, curbal, 
                           balance, rate, depdte, matdte, term, matid, rmaindt, intpay))

con.execute("DROP TABLE arfd")
con.execute("""
    CREATE TEMP TABLE arfd AS
    SELECT * FROM (VALUES 
        (NULL::BIGINT, NULL::BIGINT, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, 
         NULL::INTEGER, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR,
         NULL::VARCHAR, NULL::INTEGER, NULL::VARCHAR, NULL::INTEGER, NULL::DOUBLE)
    ) t(acctno, custno, custname, fundmne, fundname, pmtyp, curbal, balance, rate,
        depdte, matdte, term, matid, rmaindt, intpay)
    WHERE 1=0
""")

con.executemany("INSERT INTO arfd VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", updated_records)

con.execute("CREATE TEMP TABLE arfd1 AS SELECT * FROM arfd WHERE pmtyp = 1 ORDER BY fundname")
con.execute("CREATE TEMP TABLE arfd2 AS SELECT * FROM arfd WHERE pmtyp = 2 ORDER BY fundname")

con.execute("CREATE TEMP TABLE sumarfd1 AS SELECT SUM(term) term, SUM(rmaindt) rmaindt, SUM(curbal) curbal, SUM(balance) balance FROM arfd1")
con.execute("CREATE TEMP TABLE sumarfd2 AS SELECT SUM(term) term, SUM(rmaindt) rmaindt, SUM(curbal) curbal, SUM(balance) balance FROM arfd2")
con.execute("CREATE TEMP TABLE totarfd AS SELECT SUM(term) term, SUM(rmaindt) rmaindt, SUM(curbal) curbal, SUM(balance) balance FROM arfd")

with open(OUTPUT_DIR / 'artb_report.txt', 'a') as f:
    f.write("\nTABLE 2: DETAILED BREAKDOWN LISTING FOR AMANAH RAYA GROUP\n")
    f.write("I) CURRENT ACCOUNT\n")
    f.write("CIS NO.      CUST. MNEMONIC  CUSTOMER FULL NAME" + " "*90 + "CURRENT BALANCE  INTEREST (%)  BALANCE\n")
    
    for dataset, label in [('arca1', 'A'), ('arca2', 'B')]:
        ftype = 'PUBLIC' if label == 'A' else 'NON-PUBLIC'
        f.write(f"\n({label}) {ftype} MUTUAL FUND PORTFOLIO\n")
        
        for row in con.execute(f"SELECT * FROM {dataset}").fetchall():
            custno, fundmne, fundname, curbal, intrate, balance = row[1], row[3], row[4], row[6], row[8], row[7]
            f.write(f"{custno:<12} {fundmne:<15} {fundname:<120} {curbal:18.2f} {intrate:13.2f} {balance:18.2f}\n")
        
        sumrow = con.execute(f"SELECT * FROM sum{dataset}").fetchone()
        f.write(f"\nSUBTOTAL ({label}){' '*120} {sumrow[0]:18.2f} {'-':>13} {sumrow[1]:18.2f}\n")
    
    totrow = con.execute("SELECT * FROM totarca").fetchone()
    f.write(f"\nTOTAL (A)+(B){' '*120} {totrow[0]:18.2f} {'-':>13} {totrow[1]:18.2f}\n")
    
    f.write("\n\nII) FIXED DEPOSITS\n")
    f.write("CIS NO.      CUST. MNEMONIC  CUSTOMER FULL NAME" + " "*30 + "TRANSACTION DATE  MATURITY DATE  ORIGINAL TENOR  REMAINING DAYS  CURRENT BALANCE  INTEREST (%)  BALANCE\n")
    
    for dataset, label in [('arfd1', 'C'), ('arfd2', 'D')]:
        ftype = 'PUBLIC' if label == 'C' else 'NON-PUBLIC'
        f.write(f"\n({label}) {ftype} MUTUAL FUND PORTFOLIO\n")
        
        fund_totals = {}
        for row in con.execute(f"SELECT * FROM {dataset}").fetchall():
            custno, fundmne, fundname, curbal, balance, rate, depdte, matdte, term, matid, rmaindt = \
                row[1], row[3], row[4], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]
            
            f.write(f"{custno:<12} {fundmne:<15} {fundname:<40} {depdte:<17} {matdte:<14} {term:>3} {matid} {rmaindt:>14} {curbal:18.2f} {rate:13.2f} {balance:18.2f}\n")
            
            if fundname not in fund_totals:
                fund_totals[fundname] = {'term': 0, 'rmaindt': 0, 'curbal': 0, 'balance': 0}
            fund_totals[fundname]['term'] += term
            fund_totals[fundname]['rmaindt'] += rmaindt
            fund_totals[fundname]['curbal'] += curbal
            fund_totals[fundname]['balance'] += balance
        
        sumrow = con.execute(f"SELECT * FROM sum{dataset}").fetchone()
        f.write(f"\nSUBTOTAL ({label}){' '*70} {sumrow[0]:>3} M {sumrow[1]:>14} {sumrow[2]:18.2f} {'-':>13} {sumrow[3]:18.2f}\n")
    
    totrow = con.execute("SELECT * FROM totarfd").fetchone()
    f.write(f"\nTOTAL (C)+(D){' '*70} {totrow[0]:>3} M {totrow[1]:>14} {totrow[2]:18.2f} {'-':>13} {totrow[3]:18.2f}\n")
    
    gtrow = con.execute("SELECT 0 term, 0 rmaindt, SUM(curbal) curbal, SUM(balance) balance FROM (SELECT * FROM totarca UNION ALL SELECT * FROM totarfd)").fetchone()
    f.write(f"\nGRAND TOTAL (A+B+C+D){' '*55} {totrow[0]:>3} M {totrow[1]:>14} {gtrow[2]:18.2f} {'-':>13} {gtrow[3]:18.2f}\n")

print(f"""
Amanah Raya Trustees Report Complete

Summary:
- PM Funds: {con.execute('SELECT COUNT(*) FROM cafd_final WHERE pmtyp = 1').fetchone()[0]} accounts
- Non-PM Funds: {con.execute('SELECT COUNT(*) FROM cafd_final WHERE pmtyp = 2').fetchone()[0]} accounts
- Current Accounts: {con.execute('SELECT COUNT(*) FROM arca').fetchone()[0]}
- Fixed Deposits: {con.execute('SELECT COUNT(*) FROM arfd').fetchone()[0]}

Output Files:
- cafd_artb.parquet - Complete ARTB account data
- artb_report.txt - Formatted report
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
