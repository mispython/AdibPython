#!/usr/bin/env python3
"""
EIBD2CCR - Daily Credit Report
Generates daily credit file with overdraft days and payment tracking
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/loan/reptdate.parquet')").fetchone()[0]
days, months, years = reptdate.day, reptdate.month, reptdate.year
sdate = int(reptdate.strftime('%y%m%d'))
reptyear = reptdate.year % 100
reptmon = reptdate.month
reptday = reptdate.day
rdate = reptdate.strftime('%Y%m%d')
reptmon2 = (reptdate - timedelta(days=reptdate.day)).month

print(f"Daily Credit Report - {reptdate.strftime('%d/%m/%Y')}")

con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT *, 'L' acctind
    FROM read_parquet('{INPUT_DIR}/ccrisp/loan.parquet')
""")

con.execute(f"""
    CREATE TEMP TABLE overdfs_raw AS
    SELECT acctno, product, openind, closedt, curbal, riskcode, sector, branch, noteno
    FROM read_parquet('{INPUT_DIR}/deposit/current.parquet')
""")

con.execute("""
    CREATE TEMP TABLE overdfs AS
    SELECT 
        acctno, product, openind, closedt, curbal, riskcode,
        sector sector1, branch, 0 noteno, 'D' acctind,
        CASE 
            WHEN riskcode IN ('1','2','3','4') OR curbal < 0 OR product BETWEEN 30 AND 34
            THEN 'Y' ELSE 'N'
        END taken
    FROM overdfs_raw
    WHERE openind NOT IN ('B','C','P') OR (openind IN ('B','C','P') AND closedt > 0)
""")

con.execute("CREATE TEMP TABLE overdfs_dedup AS SELECT DISTINCT * FROM overdfs ORDER BY acctno")

con.execute("""
    CREATE TEMP TABLE loan_all AS
    SELECT 
        acctno, noteno, exoddate, tempoddt, curbal curbalcris, openind, taken,
        paidind, loantype, spread, usmargin, odstat, bldate, branch branch1, acctind
    FROM loan
    UNION ALL
    SELECT 
        acctno, noteno, 0 exoddate, 0 tempoddt, curbal curbalcris, openind, taken,
        '' paidind, 0 loantype, 0.0 spread, 0.0 usmargin, '' odstat, 0 bldate, 
        branch branch1, acctind
    FROM overdfs_dedup
""")

con.execute(f"""
    CREATE TEMP TABLE dloan AS
    SELECT *, f5acconv flag5
    FROM read_parquet('{INPUT_DIR}/loan/ln{reptyear:02d}{reptmon:02d}{reptday:02d}.parquet')
""")

con.execute("""
    CREATE TEMP TABLE loan_merged AS
    SELECT 
        COALESCE(d.acctno, l.acctno) acctno, COALESCE(d.noteno, l.noteno) noteno,
        l.exoddate, l.tempoddt, l.curbalcris, l.openind, l.taken,
        l.paidind, l.loantype, l.spread, l.usmargin, l.odstat, l.bldate,
        COALESCE(d.branch, l.branch1) branch,
        d.balance, d.borstat, d.oldnotedayarr, d.reaccrual, d.loanstat, d.feeamt, d.flag5
    FROM loan_all l
    LEFT JOIN dloan d ON l.acctno = d.acctno AND l.noteno = d.noteno
    WHERE d.acctno IS NOT NULL OR l.taken = 'Y'
""")

def calculate_nodays(row):
    acctno, noteno, exoddate, tempoddt, curbalcris, openind, paidind, bldate, \
    borstat, oldnotedayarr, loantype, flag5 = row[:12]
    nodays = 0
    
    if (exoddate != 0 or tempoddt != 0) and (curbalcris <= 0):
        try:
            exoddt = datetime.strptime(str(exoddate)[:8], '%m%d%Y') if exoddate else None
            tempdt = datetime.strptime(str(tempoddt)[:8], '%m%d%Y') if tempoddt else None
            
            if exoddt and tempdt:
                oddate = min(exoddt, tempdt)
            elif exoddate == 0:
                oddate = tempdt
            else:
                oddate = exoddt
            
            if oddate:
                nodays = (reptdate - oddate).days + 1
        except:
            pass
    
    if openind in ('P', 'C', 'B'):
        nodays = 0
    if paidind == 'P':
        nodays = 0
    
    if bldate and bldate > 0:
        try:
            bl_date = datetime.strptime(str(bldate)[:8], '%m%d%Y')
            nodays = (reptdate - bl_date).days
        except:
            pass
    
    if nodays < 0:
        nodays = 0
    
    if loantype in (981, 982, 983, 991, 992, 993):
        if loantype in (983, 993) and paidind == 'P':
            nodays = 0
    
    if borstat == 'F':
        if bldate and bldate > 0:
            try:
                bl_date = datetime.strptime(str(bldate)[:8], '%m%d%Y')
                nodays = (reptdate - bl_date).days
            except:
                pass
        if nodays < 0:
            nodays = 0
    
    if oldnotedayarr and oldnotedayarr > 0 and 98000 <= noteno <= 98999:
        nodays += oldnotedayarr
    
    if loantype in (128, 130, 380, 381, 700, 705, 720, 725) and noteno >= 98000:
        if paidind == 'P':
            nodays = 0
    
    return nodays

loans = con.execute("SELECT * FROM loan_merged").fetchall()
loan_days = [((*row, calculate_nodays(row))) for row in loans]

con.execute("""
    CREATE TEMP TABLE loan_with_days AS
    SELECT acctno, noteno, exoddate, tempoddt, curbalcris, openind, taken,
           paidind, loantype, spread, usmargin, odstat, bldate, branch,
           balance, borstat, oldnotedayarr, reaccrual, loanstat, feeamt, flag5, 0 nodays
    FROM loan_merged
""")

con.executemany("UPDATE loan_with_days SET nodays = ? WHERE acctno = ? AND noteno = ?", 
                [(row[-1], row[0], row[1]) for row in loan_days])

con.execute(f"""
    CREATE TEMP TABLE prevloan AS
    SELECT acctno, noteno, curbal pcurbal
    FROM read_parquet('{INPUT_DIR}/bnm/loan{reptmon2:02d}4.parquet')
""")

con.execute("""
    CREATE TEMP TABLE loan_with_prev AS
    SELECT l.*, COALESCE(p.pcurbal, 0) pcurbal
    FROM loan_with_days l
    LEFT JOIN prevloan p ON l.acctno = p.acctno AND l.noteno = p.noteno
""")

trncd = (610, 614, 615, 619, 661, 680, 800, 801, 810)
weeks_to_load = 1 if reptday <= 8 else 2 if reptday <= 15 else 3 if reptday <= 22 else 4

for wk in range(1, weeks_to_load + 1):
    con.execute(f"""
        CREATE TEMP TABLE lntran{wk} AS
        SELECT acctno, noteno, trancode, reptdate, timectrl, tranamt
        FROM read_parquet('{INPUT_DIR}/crm/lnbtran{reptyear:02d}{reptmon:02d}{wk:02d}.parquet')
        WHERE trancode IN {trncd}
    """)

union_parts = [f"SELECT * FROM lntran{i}" for i in range(1, weeks_to_load + 1)]
con.execute(f"CREATE TEMP TABLE lntran AS {' UNION ALL '.join(union_parts)}")

con.execute("CREATE TEMP TABLE lntran8xx AS SELECT DISTINCT acctno, noteno, trancode, reptdate, timectrl, tranamt FROM lntran WHERE trancode >= 800")
con.execute("CREATE TEMP TABLE lntran6xx AS SELECT * FROM lntran WHERE trancode < 800")
con.execute("CREATE TEMP TABLE lntran_all AS SELECT * FROM lntran8xx UNION ALL SELECT * FROM lntran6xx")
con.execute("CREATE TEMP TABLE lnpay AS SELECT acctno, noteno, SUM(tranamt) tranamt FROM lntran_all GROUP BY acctno, noteno")

con.execute("""
    CREATE TEMP TABLE loan_final AS
    SELECT l.*, COALESCE(p.tranamt, 0) tranamt,
        CASE 
            WHEN l.acctno BETWEEN 3000000000 AND 3999999999 THEN
                CASE WHEN l.balance + (-1) * l.pcurbal >= 0 THEN 0 ELSE ABS(l.balance + (-1) * l.pcurbal) END
            ELSE COALESCE(p.tranamt, 0)
        END payamt
    FROM loan_with_prev l
    LEFT JOIN lnpay p ON l.acctno = p.acctno AND l.noteno = p.noteno
""")

con.execute("UPDATE loan_final SET payamt = 0 WHERE payamt < 0 OR payamt IS NULL")

con.execute(f"""
    CREATE TEMP TABLE daycred AS
    SELECT branch, acctno, noteno, COALESCE(balance, 0) balance, '{rdate}' date, nodays, payamt,
           COALESCE(usmargin, 0) usmargin, COALESCE(reaccrual, '') reaccrual, COALESCE(spread, 0) spread,
           COALESCE(loanstat, '') loanstat, COALESCE(odstat, '') odstat, COALESCE(feeamt, 0) feeamt,
           COALESCE(bldate, 0) bldate, COALESCE(exoddate, 0) exoddate, COALESCE(flag5, '') flag5,
           COALESCE(paidind, '') paidind
    FROM loan_final
    ORDER BY branch, acctno, noteno
""")

with open(OUTPUT_DIR / 'daycred.txt', 'w') as f:
    for row in con.execute("SELECT * FROM daycred").fetchall():
        branch, acctno, noteno, balance, date, nodays, payamt, usmargin, \
        reaccrual, spread, loanstat, odstat, feeamt, bldate, exoddate, flag5, paidind = row
        
        bldt = f"{bldate:08d}" if bldate else "        "
        oddt = f"{exoddate:08d}" if exoddate else "        "
        
        line = (f"{branch:05d}{acctno:010d}{noteno:010d}{balance:017.2f}{date:>8}{nodays:06d}"
                f"{payamt:017.2f}{usmargin:05.3f}{reaccrual:1}{spread:05.3f}{loanstat:1}"
                f"{odstat:2}{feeamt:08.2f}{bldt:8}{oddt:8}{flag5:1}{paidind:1}\n")
        f.write(line)

con.execute(f"COPY daycred TO '{OUTPUT_DIR}/daycred{rdate}.parquet'")
con.execute(f"COPY daycred TO '{OUTPUT_DIR}/daycred{rdate}.csv' (HEADER, DELIMITER ',')")

record_count = con.execute("SELECT COUNT(*) FROM daycred").fetchone()[0]

print(f"""
Output: {record_count} records
Files: daycred.txt (127 bytes/record), daycred{rdate}.parquet, daycred{rdate}.csv
Weeks loaded: {weeks_to_load}
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
