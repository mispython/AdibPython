# ================================================================
# Job Name : EIBDRNID_RENID_PROCESS
# Purpose  : Process RNID & TRANCHE files, enrich with account data,
#            segregate by COSTCTR, and output monthly & daily tables
# Libraries: duckdb, pyarrow
# ================================================================

import duckdb
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import date, timedelta

# ------------------------------------------------
# 1. Report Date Setup
# ------------------------------------------------
reptdate = date.today() - timedelta(days=1)
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
reptdt = reptdate.strftime("%y%m%d")

# ------------------------------------------------
# 2. Connect to DuckDB
# ------------------------------------------------
con = duckdb.connect(database=':memory:')

# ------------------------------------------------
# 3. Load RNID File
# ------------------------------------------------
rnid_tbl = pv.read_csv(
    "RNID.dat",
    parse_options=pv.ParseOptions(delimiter="|", quote_char=False),  # adjust delimiter if needed
)

con.register("RNID_SRC", rnid_tbl)

con.execute("""
CREATE TABLE RNID AS
SELECT
    CAST(column1 AS BIGINT) AS NID_ACCTNO,
    column2 AS NID_REFNO,
    CAST(column3 AS BIGINT) AS ACCTNO,
    column4 AS NID_CDNO,
    column5 AS CUSTNAME,
    column6 AS NEWIC,
    CAST(column7 AS INT) AS BRANCH,
    CAST(column8 AS INT) AS CUSTCODE,
    column9 AS TRANCHENO,
    column10 AS DATE AS STARTDT,
    column11 AS DATE AS MATDT,
    CAST(column12 AS DOUBLE) AS INTRATE,
    CAST(column13 AS DOUBLE) AS INTPLTDRATE,
    CAST(column14 AS DOUBLE) AS CURBAL,
    column15 AS DATE AS LSTINTPAYDT,
    column16 AS DATE AS NXTINTPAYDT,
    column17 AS INTFRQ,
    column18 AS NIDSTAT,
    CAST(column19 AS DOUBLE) AS ACCINT,
    column20 AS CDSTAT,
    column21 AS POSTSTAT,
    column22 AS DATE AS EARLY_WDDT,
    CAST(column23 AS DOUBLE) AS EARLY_WDPROC,
    column24 AS EARLY_TELRID,
    column25 AS EARLY_OFFID1,
    column26 AS EARLY_OFFID2,
    column27 AS DATE AS CANCDT,
    column28 AS CANC_TELRID,
    column29 AS CANC_OFFID1,
    column30 AS CANC_OFFID2,
    column31 AS DATE AS APPLDT,
    column32 AS APPLDTTM,
    column33 AS APPL_TELRID,
    column34 AS APPL_OFFID1,
    column35 AS APPL_OFFID2,
    column36 AS DATE AS PRINTDT,
    column37 AS PRINT_TELRID,
    column38 AS PRINT_OFFID1,
    column39 AS PRINT_OFFID2,
    column40 AS DATE AS MAINTDT,
    column41 AS MAINT_TELRID,
    column42 AS MAINT_OFFID1,
    column43 AS MAINT_OFFID2,
    column44 AS PLEDGESTAT,
    CAST(column45 AS BIGINT) AS NID_ODACCTNO,
    CAST(column46 AS BIGINT) AS CCOLLNO,
    column47 AS DATE AS PLEDGEDT,
    column48 AS PLEDGE_TELRID,
    column49 AS PLEDGE_OFFID1,
    column50 AS PLEDGE_OFFID2,
    column51 AS DATE AS CPLEDGEDT,
    column52 AS CPLEDGE_TELRID,
    column53 AS CPLEDGE_OFFID1,
    column54 AS CPLEDGE_OFFID2,
    CAST(column55 AS INT) AS TERM,
    column56 AS SACODE,
    column57 AS STAFFID,
    column58 AS TERMID,
    CAST(column59 AS DOUBLE) AS INTPD,
    CAST(column60 AS INT) AS COSTCTR,
    CAST(column61 AS INT) AS PRODUCT,
    'MYR' AS CURCODE
FROM RNID_SRC
""")

# ------------------------------------------------
# 4. Load MNI Accounts (Current + Saving)
# ------------------------------------------------
# (Assume preloaded CSV/Parquet for MNITB and IMNITB tables)
con.execute("""
CREATE TABLE MNIACC AS
SELECT ACCTNO, LPAD(CAST(CUSTCODE AS VARCHAR), 2, '0') AS CUSTCD
FROM (
    SELECT * FROM MNITB_CURRENT
    UNION ALL
    SELECT * FROM MNITB_SAVING
    UNION ALL
    SELECT * FROM IMNITB_CURRENT
    UNION ALL
    SELECT * FROM IMNITB_SAVING
)
""")

# ------------------------------------------------
# 5. Merge RNID with MNIACC
# ------------------------------------------------
con.execute(f"""
CREATE TABLE RNID_MERGED AS
SELECT R.*, 
       COALESCE(M.CUSTCD, LPAD(CAST(R.CUSTCODE AS VARCHAR),2,'0')) AS CUSTCD
FROM RNID R
LEFT JOIN MNIACC M USING(ACCTNO)
""")

# Split into DEPO and IDEPO depending on COSTCTR
con.execute(f"""
CREATE TABLE DEPO_RNIDM{reptmon} AS
SELECT * FROM RNID_MERGED WHERE NOT (COSTCTR BETWEEN 3000 AND 4999)
""")

con.execute(f"""
CREATE TABLE IDEPO_RNIDM{reptmon} AS
SELECT * FROM RNID_MERGED WHERE (COSTCTR BETWEEN 3000 AND 4999)
""")

# ------------------------------------------------
# 6. Load TRANCHE File
# ------------------------------------------------
trnch_tbl = pv.read_csv("TRNCH.dat")
con.register("TRNCH_SRC", trnch_tbl)

con.execute("""
CREATE TABLE TRNCH AS
SELECT
    column1 AS TRANCHENO,
    column2 AS TRN_STAT,
    column3::DATE AS TRN_OFFERSTARTDT,
    column4::DATE AS TRN_OFFERENDDT,
    column5::DATE AS TRNSTARTDT,
    column6::DATE AS TRNMATDT,
    CAST(column7 AS INT) AS TERM,
    column8 AS TERMID,
    column9 AS INTFRQ,
    CAST(column10 AS DOUBLE) AS INTRATE,
    column11::DATE AS CREATDT,
    column12 AS CREATSTAFFID,
    column13::DATE AS MAINTDT,
    column14 AS MAINTSTAFFID,
    CAST(column15 AS DOUBLE) AS INTPLRATE_BID,
    CAST(column16 AS DOUBLE) AS INTPLRATE_OFFER
FROM TRNCH_SRC
""")

# ------------------------------------------------
# 7. Outputs (parquet for downstream)
# ------------------------------------------------
pq.write_table(con.execute(f"SELECT * FROM DEPO_RNIDM{reptmon}").arrow(), f"DEPO_RNIDM{reptmon}.parquet")
pq.write_table(con.execute(f"SELECT * FROM IDEPO_RNIDM{reptmon}").arrow(), f"IDEPO_RNIDM{reptmon}.parquet")
pq.write_table(con.execute("SELECT * FROM TRNCH").arrow(), f"TRNCH_{reptday}.parquet")
