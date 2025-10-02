# ================================================================
# Job Name : EIIDDPFD_FIXED_DEPOSIT_PROCESS
# Purpose  : Process Fixed Deposit accounts (FD) with CIS, CRMFD,
#            and signature data. Handle monthly/week logic.
# Libraries: duckdb, pyarrow
# ================================================================

import duckdb
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import date, timedelta

# ------------------------------------------------
# 1. Report Date Setup
# ------------------------------------------------
reptdate = date.today() - timedelta(days=1)
wk_day = reptdate.day

if 1 <= wk_day <= 8:
    wk = "1"
elif 9 <= wk_day <= 15:
    wk = "2"
elif 16 <= wk_day <= 22:
    wk = "3"
else:
    wk = "4"

# Adjust MM logic
mm = reptdate.month if wk == "4" else reptdate.month - 1
if mm == 0:
    mm = 12

reptyear = f"{reptdate.year % 100:02d}"
reptmon = f"{reptdate.month:02d}"
reptmon1 = f"{mm:02d}"
reptday = f"{wk_day:02d}"
rdate = reptdate.strftime("%d%m%Y")
nowk = wk

# ------------------------------------------------
# 2. Connect DuckDB
# ------------------------------------------------
con = duckdb.connect(database=":memory:")

# ------------------------------------------------
# 3. CIS (customer info)
# ------------------------------------------------
con.execute("""
CREATE OR REPLACE TABLE CIS AS
SELECT
    ACCTNO,
    CITIZEN AS COUNTRY,
    CAST(substr(BIRTHDAT,1,2) AS INT) AS BDD,
    CAST(substr(BIRTHDAT,3,2) AS INT) AS BMM,
    CAST(substr(BIRTHDAT,5,4) AS INT) AS BYY,
    CUSTNAM1 AS NAME,
    CASE WHEN SECCUST = '901'
         THEN MAKE_DATE(CAST(substr(BIRTHDAT,5,4) AS INT),
                        CAST(substr(BIRTHDAT,3,2) AS INT),
                        CAST(substr(BIRTHDAT,1,2) AS INT))
    END AS DOBCIS,
    RACE
FROM CIS_DEPOSIT
WHERE SECCUST = '901'
""")

con.execute("CREATE OR REPLACE TABLE CIS AS SELECT DISTINCT * FROM CIS")

# ------------------------------------------------
# 4. Monthly FD logic
# ------------------------------------------------
if nowk != "4" and reptmon == "01":
    # Case 1: Reset YTD Fee
    con.execute("""
    CREATE OR REPLACE TABLE FD AS
    SELECT *, 0 AS FEEYTD
    FROM DPTRBL_FD
    """)
else:
    # Case 2: Merge CRMFD and FD
    con.execute(f"""
    CREATE OR REPLACE TABLE CRMFD AS
    SELECT DISTINCT * FROM MNICRM_FD{reptmon1}
    """)
    con.execute("""
    CREATE OR REPLACE TABLE FD AS
    SELECT A.*, B.*
    FROM DPTRBL_FD A
    LEFT JOIN (SELECT * EXCLUDE (COSTCTR) FROM CRMFD) B
    USING (ACCTNO)
    """)
    
# ------------------------------------------------
# 5. Process FD (apply filters & transformations)
# ------------------------------------------------
con.execute("""
CREATE OR REPLACE TABLE FDFD AS
SELECT
    ACCTNO,
    BRANCH,
    PRODUCT,
    DEPTYPE,
    ORGTYPE,
    ORGCODE,
    OPENDT,
    OPENIND,
    SECTOR,
    CUSTCODE,
    CURBAL,
    LASTTRAN,
    LEDGBAL,
    STATE,
    PURPOSE,
    AVGAMT,
    USER2,
    MTDAVBAL,
    RISKCODE,
    INTPLAN,
    INTPD,
    USER3,
    SECOND,
    INACTIVE,
    BONUTYPE,
    SERVICE,
    USER5,
    PREVBRNO,
    COSTCTR,
    FEEYTD,
    INTYTD,
    YTDAVAMT,
    BONUSANO,
    CLOSEDT,
    INTPAYBL,
    DTLSTCUST,
    DOBMNI,
    AVGBAL,
    INTRSTPD,
    DNBFISME,
    MAILCODE,
    INTCYCODE,
    substr(STATCD,4,1) AS ACSTATUS4,
    substr(STATCD,5,1) AS ACSTATUS5,
    substr(STATCD,6,1) AS ACSTATUS6,
    substr(STATCD,7,1) AS ACSTATUS7,
    substr(STATCD,8,1) AS ACSTATUS8,
    substr(STATCD,9,1) AS ACSTATUS9,
    substr(STATCD,10,1) AS ACSTATUS10,
    POST_IND,
    COALESCE(PSREASON,REASON) AS REASON,
    INSTRUCTIONS,
    REOPENDT,
    STMT_CYCLE,
    NXT_STMT_CYCLE_DT,
    PRIN_ACCT
FROM FD
WHERE OPENIND NOT IN ('B','C','P')
  AND (PRODUCT < 350 OR PRODUCT > 357)
""")

# ------------------------------------------------
# 6. Merge with CIS, SAACC, MISMTD
# ------------------------------------------------
con.execute(f"""
CREATE OR REPLACE TABLE FIXED_IFD{reptyear}{reptmon}{reptday} AS
SELECT
    A.ACCTNO,
    A.*,
    C.COUNTRY,
    C.DOBCIS,
    C.RACE,
    COALESCE(S.ESIGNATURE,'N') AS ESIGNATURE
FROM FDFD A
LEFT JOIN CIS C USING(ACCTNO)
LEFT JOIN SIGNA_SMSACC S USING(ACCTNO)
LEFT JOIN MISMTD_FAVG{reptmon} M USING(ACCTNO)
""")

# ------------------------------------------------
# 7. Export Outputs
# ------------------------------------------------
pq.write_table(
    con.execute(f"SELECT * FROM FIXED_IFD{reptyear}{reptmon}{reptday}").arrow(),
    f"FIXED_IFD{reptyear}{reptmon}{reptday}.parquet"
)
