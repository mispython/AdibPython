#!/usr/bin/env python3
"""
EIBDMSX1 - Daily MIS Summary
Generates daily position reports for FD, Savings, and Current accounts
"""

import duckdb
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/mnitb/reptdate.parquet')").fetchone()[0]
reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
rdate = int(reptdate.strftime('%y%m%d'))

print(f"Daily MIS Summary - {reptdate.strftime('%d/%m/%Y')}")

# ============================================================================
# CONVENTIONAL FIXED DEPOSIT (FD)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE fd AS
    SELECT 
        {rdate} reptdate,
        curbal,
        product,
        custcode,
        CASE 
            WHEN product = 395 OR custcode IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END p395bal,
        CASE 
            WHEN product = 395 AND custcode IN (77,78,95,96) THEN curbal ELSE 0 
        END p395bali,
        CASE 
            WHEN product = 395 AND custcode NOT IN (77,78,95,96) THEN curbal ELSE 0 
        END p395balc,
        CASE 
            WHEN product != 395 AND product != 397 AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            THEN curbal ELSE 0 
        END totfd,
        CASE 
            WHEN product != 395 AND product != 397 AND custcode IN (77,78,95,96) 
            AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END x395bali,
        CASE 
            WHEN product != 395 AND product != 397 AND custcode NOT IN (77,78,95,96) 
            AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END x395balc
    FROM read_parquet('{INPUT_DIR}/mnitb/fd.parquet')
    WHERE openind NOT IN ('B','C','P') AND curbal >= 0 AND product != 395
""")

con.execute("""
    CREATE TEMP TABLE fdc AS
    SELECT 
        reptdate,
        SUM(totfd) totfd,
        SUM(x395bali) x395bali,
        SUM(x395balc) x395balc,
        SUM(p395bal) p395bal,
        SUM(p395bali) p395bali,
        SUM(p395balc) p395balc
    FROM fd
    GROUP BY reptdate
""")

# ============================================================================
# ISLAMIC FIXED DEPOSIT (FDI)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE fdi AS
    SELECT 
        {rdate} reptdate,
        curbal,
        product,
        custcode,
        CASE 
            WHEN product IN (396,394,393) OR custcode IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END p396bal,
        CASE 
            WHEN product IN (396,394,393) AND custcode IN (77,78,95,96) THEN curbal ELSE 0 
        END p396bali,
        CASE 
            WHEN product IN (396,394,393) AND custcode NOT IN (77,78,95,96) THEN curbal ELSE 0 
        END p396balc,
        CASE 
            WHEN product NOT IN (396,394,393,398) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            THEN curbal ELSE 0 
        END totfdi,
        CASE 
            WHEN product NOT IN (396,394,393,398) AND custcode IN (77,78,95,96) 
            AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END x396bali,
        CASE 
            WHEN product NOT IN (396,394,393,398) AND custcode NOT IN (77,78,95,96) 
            AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END x396balc
    FROM read_parquet('{INPUT_DIR}/mniib/fd.parquet')
    WHERE openind NOT IN ('B','C','P') AND curbal >= 0
""")

con.execute("""
    CREATE TEMP TABLE fdi_sum AS
    SELECT 
        reptdate,
        SUM(totfdi) totfdi,
        SUM(x396bali) x396bali,
        SUM(x396balc) x396balc,
        SUM(p396bal) p396bal,
        SUM(p396bali) p396bali,
        SUM(p396balc) p396balc
    FROM fdi
    GROUP BY reptdate
""")

# ============================================================================
# CONVENTIONAL SAVINGS (SAVG)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE savg AS
    SELECT 
        {rdate} reptdate,
        branch,
        product,
        CASE WHEN curbal >= 0 THEN curbal ELSE 0 END totsavg,
        1 sacnt
    FROM read_parquet('{INPUT_DIR}/mnitb/saving.parquet')
    WHERE openind NOT IN ('B','C','P') AND product NOT IN (150,151,152,177,181)
""")

con.execute("""
    CREATE TEMP TABLE sac AS
    SELECT reptdate, SUM(totsavg) totsavg
    FROM savg
    GROUP BY reptdate
""")

con.execute("""
    CREATE TEMP TABLE sacbr AS
    SELECT reptdate, branch, SUM(totsavg) totsavg, SUM(sacnt) sacnt
    FROM savg
    GROUP BY reptdate, branch
""")

# ============================================================================
# ISLAMIC SAVINGS (SAVGI)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE savgi AS
    SELECT 
        {rdate} reptdate,
        branch,
        product,
        CASE WHEN curbal >= 0 THEN curbal ELSE 0 END totsavgi,
        CASE WHEN product = 214 AND curbal >= 0 THEN curbal ELSE 0 END totmbsa,
        1 isacnt
    FROM read_parquet('{INPUT_DIR}/mniib/saving.parquet')
    WHERE openind NOT IN ('B','C','P')
""")

con.execute("""
    CREATE TEMP TABLE sai AS
    SELECT reptdate, SUM(totsavgi) totsavgi, SUM(totmbsa) totmbsa
    FROM savgi
    GROUP BY reptdate
""")

con.execute("""
    CREATE TEMP TABLE saibr AS
    SELECT reptdate, branch, SUM(totsavgi) totsavgi, SUM(isacnt) isacnt
    FROM savgi
    GROUP BY reptdate, branch
""")

# ============================================================================
# CONVENTIONAL CURRENT ACCOUNTS (CURR)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE curr AS
    SELECT 
        {rdate} reptdate,
        product,
        custcode,
        acctno,
        curbal,
        CASE WHEN product = 104 THEN '02' WHEN product = 105 THEN '81' ELSE custcode::VARCHAR END custfiss,
        CASE WHEN product IN (68,69,72) AND custcode IN (77,78,95,96) THEN curbal ELSE 0 END p068bali,
        CASE WHEN product IN (68,69,72) AND custcode NOT IN (77,78,95,96) THEN curbal ELSE 0 END p068balc,
        CASE WHEN product = 104 THEN curbal ELSE 0 END totvosc,
        CASE WHEN product = 105 THEN curbal ELSE 0 END totvosf,
        CASE 
            WHEN product IN (68,69,72) OR custcode IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END p068bal,
        CASE WHEN product IN (150,151,152,181) THEN curbal ELSE 0 END aceca,
        CASE 
            WHEN product NOT IN (68,69,72,79,150,151,152,181) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            THEN curbal ELSE 0 
        END totdmnd,
        CASE 
            WHEN custcode IN (77,78,95,96) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            AND product NOT IN (68,69,72,150,151,152,181) THEN curbal ELSE 0 
        END x068bali,
        CASE 
            WHEN custcode NOT IN (77,78,95,96) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            AND product NOT IN (68,69,72,150,151,152,181) THEN curbal ELSE 0 
        END x068balc
    FROM read_parquet('{INPUT_DIR}/mnitb/current.parquet')
    WHERE openind NOT IN ('B','C','P') 
      AND curbal >= 0
      AND product NOT IN (30,31,34,79)
      AND NOT (acctno BETWEEN 3590000000 AND 3599999999)
""")

con.execute("""
    CREATE TEMP TABLE cac AS
    SELECT 
        reptdate,
        SUM(p068bal) p068bal,
        SUM(p068bali) p068bali,
        SUM(p068balc) p068balc,
        SUM(aceca) aceca,
        SUM(x068bali) x068bali,
        SUM(totvosc) totvosc,
        SUM(totvosf) totvosf,
        SUM(totdmnd) totdmnd,
        SUM(x068balc) x068balc
    FROM curr
    GROUP BY reptdate
""")

# ============================================================================
# ISLAMIC CURRENT ACCOUNTS (CURRI)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE curri AS
    SELECT 
        {rdate} reptdate,
        product,
        custcode,
        acctno,
        curbal,
        CASE WHEN product = 104 THEN '02' WHEN product = 105 THEN '81' ELSE custcode::VARCHAR END custfiss,
        CASE 
            WHEN product IN (66,67) OR custcode IN (01,02,03,07,10,12,81,82,83,84) THEN curbal ELSE 0 
        END p066bal,
        CASE WHEN product IN (66,67) AND custcode IN (77,78,95,96) THEN curbal ELSE 0 END p066bali,
        CASE WHEN product IN (66,67) AND custcode NOT IN (77,78,95,96) THEN curbal ELSE 0 END p066balc,
        CASE 
            WHEN product NOT IN (66,67) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            THEN curbal ELSE 0 
        END totdmndi,
        CASE 
            WHEN custcode IN (77,78,95,96) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            AND product NOT IN (66,67) THEN curbal ELSE 0 
        END x066bali,
        CASE 
            WHEN custcode NOT IN (77,78,95,96) AND custcode NOT IN (01,02,03,07,10,12,81,82,83,84) 
            AND product NOT IN (66,67) THEN curbal ELSE 0 
        END x066balc
    FROM read_parquet('{INPUT_DIR}/mniib/current.parquet')
    WHERE openind NOT IN ('B','C','P')
      AND curbal >= 0
      AND product NOT IN (32,33,80)
      AND NOT (acctno BETWEEN 3790000000 AND 3799999999)
""")

con.execute("""
    CREATE TEMP TABLE cai AS
    SELECT 
        reptdate,
        SUM(totdmndi) totdmndi,
        SUM(p066bal) p066bal,
        SUM(p066bali) p066bali,
        SUM(p066balc) p066balc,
        SUM(x066bali) x066bali,
        SUM(x066balc) x066balc
    FROM curri
    GROUP BY reptdate
""")

# ============================================================================
# CONSOLIDATE DAILY POSITION (DYPOSN)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE dyposn AS
    SELECT 
        COALESCE(f.reptdate, fi.reptdate, s.reptdate, si.reptdate, c.reptdate, ci.reptdate) reptdate,
        COALESCE(f.totfd, 0) totfd,
        COALESCE(f.x395bali, 0) x395bali,
        COALESCE(f.x395balc, 0) x395balc,
        COALESCE(f.p395bal, 0) p395bal,
        COALESCE(f.p395bali, 0) p395bali,
        COALESCE(f.p395balc, 0) p395balc,
        COALESCE(fi.totfdi, 0) totfdi,
        COALESCE(fi.x396bali, 0) x396bali,
        COALESCE(fi.x396balc, 0) x396balc,
        COALESCE(fi.p396bal, 0) p396bal,
        COALESCE(fi.p396bali, 0) p396bali,
        COALESCE(fi.p396balc, 0) p396balc,
        COALESCE(s.totsavg, 0) totsavg,
        COALESCE(si.totsavgi, 0) totsavgi,
        COALESCE(si.totmbsa, 0) totmbsa,
        COALESCE(c.p068bal, 0) p068bal,
        COALESCE(c.p068bali, 0) p068bali,
        COALESCE(c.p068balc, 0) p068balc,
        COALESCE(c.aceca, 0) aceca,
        COALESCE(c.x068bali, 0) x068bali,
        COALESCE(c.totvosc, 0) totvosc,
        COALESCE(c.totvosf, 0) totvosf,
        COALESCE(c.totdmnd, 0) totdmnd,
        COALESCE(c.x068balc, 0) x068balc,
        COALESCE(ci.totdmndi, 0) totdmndi,
        COALESCE(ci.p066bal, 0) p066bal,
        COALESCE(ci.p066bali, 0) p066bali,
        COALESCE(ci.p066balc, 0) p066balc,
        COALESCE(ci.x066bali, 0) x066bali,
        COALESCE(ci.x066balc, 0) x066balc
    FROM fdc f
    FULL OUTER JOIN fdi_sum fi ON f.reptdate = fi.reptdate
    FULL OUTER JOIN sac s ON f.reptdate = s.reptdate
    FULL OUTER JOIN sai si ON f.reptdate = si.reptdate
    FULL OUTER JOIN cac c ON f.reptdate = c.reptdate
    FULL OUTER JOIN cai ci ON f.reptdate = ci.reptdate
""")

# ============================================================================
# CONSOLIDATE BY BRANCH (DYPOSBR)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE dyposbr AS
    SELECT 
        COALESCE(s.reptdate, si.reptdate) reptdate,
        COALESCE(s.branch, si.branch) branch,
        COALESCE(s.totsavg, 0) totsavg,
        COALESCE(s.sacnt, 0) sacnt,
        COALESCE(si.totsavgi, 0) totsavgi,
        COALESCE(si.isacnt, 0) isacnt
    FROM sacbr s
    FULL OUTER JOIN saibr si ON s.reptdate = si.reptdate AND s.branch = si.branch
""")

# ============================================================================
# DEPOSIT BALANCE BY BRANCH/TYPE (DYDPBS)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE sac13 AS
    SELECT {rdate} reptdate, 'S' dptype, branch, SUM(totsavg) balance
    FROM savg
    GROUP BY branch
""")

con.execute(f"""
    CREATE TEMP TABLE cac13 AS
    SELECT {rdate} reptdate, 'D' dptype, branch, SUM(totdmnd) balance
    FROM curr
    GROUP BY branch
""")

con.execute("""
    CREATE TEMP TABLE dydpbs AS
    SELECT * FROM sac13
    UNION ALL
    SELECT * FROM cac13
    ORDER BY reptdate, dptype, branch
""")

# ============================================================================
# SAVE OUTPUTS
# ============================================================================

if reptday == 1:
    con.execute(f"DROP TABLE IF EXISTS dyposx{reptmon:02d}")
    con.execute(f"DROP TABLE IF EXISTS dyposxbr{reptmon:02d}")
    con.execute(f"DROP TABLE IF EXISTS dydpbs{reptmon:02d}")
    con.execute(f"CREATE TABLE dyposx{reptmon:02d} AS SELECT * FROM dyposn")
    con.execute(f"CREATE TABLE dyposxbr{reptmon:02d} AS SELECT * FROM dyposbr")
    con.execute(f"CREATE TABLE dydpbs{reptmon:02d} AS SELECT * FROM dydpbs")
else:
    con.execute(f"DELETE FROM dyposx{reptmon:02d} WHERE reptdate = {rdate}")
    con.execute(f"DELETE FROM dyposxbr{reptmon:02d} WHERE reptdate = {rdate}")
    con.execute(f"DELETE FROM dydpbs{reptmon:02d} WHERE reptdate = {rdate}")
    con.execute(f"INSERT INTO dyposx{reptmon:02d} SELECT * FROM dyposn")
    con.execute(f"INSERT INTO dyposxbr{reptmon:02d} SELECT * FROM dyposbr")
    con.execute(f"INSERT INTO dydpbs{reptmon:02d} SELECT * FROM dydpbs")

con.execute(f"COPY dyposx{reptmon:02d} TO '{OUTPUT_DIR}/dyposx{reptmon:02d}.parquet'")
con.execute(f"COPY dyposxbr{reptmon:02d} TO '{OUTPUT_DIR}/dyposxbr{reptmon:02d}.parquet'")
con.execute(f"COPY dydpbs{reptmon:02d} TO '{OUTPUT_DIR}/dydpbs{reptmon:02d}.parquet'")

print(f"""
Daily MIS Summary Complete
Date: {rdate}

Outputs:
1. DYPOSX{reptmon:02d} - Daily position summary (consolidated)
2. DYPOSXBR{reptmon:02d} - Daily position by branch
3. DYDPBS{reptmon:02d} - Deposit balance by branch/type

Records:
- DYPOSN: {con.execute('SELECT COUNT(*) FROM dyposn').fetchone()[0]}
- DYPOSBR: {con.execute('SELECT COUNT(*) FROM dyposbr').fetchone()[0]}
- DYDPBS: {con.execute('SELECT COUNT(*) FROM dydpbs').fetchone()[0]}
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
