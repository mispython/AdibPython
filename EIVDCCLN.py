#!/usr/bin/env python3
"""
EIVDCCLN - CIS Customer Loan Data Extract
Processes customer information, relationships, and ownership data
"""

import duckdb
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

rptdte = datetime.now().date()
print(f"CIS Customer Loan Extract - {rptdte.strftime('%d/%m/%Y')}")

con.execute(f"""
    CREATE TEMP TABLE cisaddr AS
    SELECT 
        addref,
        addrln1, addrln2, addrln3, addrln4, addrln5,
        lstchgdd, lstchgmm, lstchgcc, lstchgyy,
        mailcode, mailstat, ctry,
        CASE WHEN ctry = 'MALAYSIA' THEN 'MY' ELSE 'OT' END country
    FROM read_csv('{INPUT_DIR}/cisaddr.txt', delim='|', header=false,
        columns={{'addref': 'VARCHAR', 'addrln1': 'VARCHAR', 'addrln2': 'VARCHAR',
                 'addrln3': 'VARCHAR', 'addrln4': 'VARCHAR', 'addrln5': 'VARCHAR',
                 'lstchgdd': 'VARCHAR', 'lstchgmm': 'VARCHAR', 'lstchgcc': 'VARCHAR',
                 'lstchgyy': 'VARCHAR', 'mailcode': 'VARCHAR', 'mailstat': 'VARCHAR',
                 'ctry': 'VARCHAR'}})
    ORDER BY addref
""")

con.execute(f"""
    CREATE TEMP TABLE cisln_raw AS
    SELECT 
        acctno, indorg, custno, gender, cacccode, seccust, citizen,
        occupat, hobbies, religion, educatn, income, marital,
        empldd, emplmm, emplcc, emplyy, hstate,
        dobdd, dobmm, dobcc, dobyy,
        emplname, lstmntdd, lstmntmm, lstmntcc, lstmntyy,
        dobdd || dobmm || dobcc || dobyy birthdat,
        opendat, race, custcons, bgc, filler, cons_old,
        ciscustcd1, ciscustcd2, ciscustcd3, ciscustcd4, ciscustcd5,
        ciscustcd6, ciscustcd7, ciscustcd8, ciscustcd9, ciscustcd10,
        ciscustcd11, ciscustcd12, ciscustcd13, ciscustcd14, ciscustcd15,
        consdd, consmm, conscc, consyy,
        sector_code, instsect, restatus, sic_iss,
        occupat_masco_cd, cust_code
    FROM read_csv('{INPUT_DIR}/cisln.txt', delim='|', header=false,
        columns={{'acctno': 'BIGINT', 'indorg': 'VARCHAR', 'custno': 'BIGINT',
                 'gender': 'VARCHAR', 'cacccode': 'VARCHAR', 'seccust': 'VARCHAR',
                 'citizen': 'VARCHAR', 'occupat': 'VARCHAR', 'hobbies': 'VARCHAR',
                 'religion': 'VARCHAR', 'educatn': 'VARCHAR', 'income': 'VARCHAR',
                 'marital': 'VARCHAR', 'empldd': 'VARCHAR', 'emplmm': 'VARCHAR',
                 'emplcc': 'VARCHAR', 'emplyy': 'VARCHAR', 'hstate': 'VARCHAR',
                 'dobdd': 'VARCHAR', 'dobmm': 'VARCHAR', 'dobcc': 'VARCHAR',
                 'dobyy': 'VARCHAR', 'emplname': 'VARCHAR', 'lstmntdd': 'VARCHAR',
                 'lstmntmm': 'VARCHAR', 'lstmntcc': 'VARCHAR', 'lstmntyy': 'VARCHAR',
                 'opendat': 'VARCHAR', 'race': 'VARCHAR', 'custcons': 'VARCHAR',
                 'bgc': 'VARCHAR', 'filler': 'VARCHAR', 'cons_old': 'VARCHAR',
                 'ciscustcd1': 'VARCHAR', 'ciscustcd2': 'VARCHAR', 'ciscustcd3': 'VARCHAR',
                 'ciscustcd4': 'VARCHAR', 'ciscustcd5': 'VARCHAR', 'ciscustcd6': 'VARCHAR',
                 'ciscustcd7': 'VARCHAR', 'ciscustcd8': 'VARCHAR', 'ciscustcd9': 'VARCHAR',
                 'ciscustcd10': 'VARCHAR', 'ciscustcd11': 'VARCHAR', 'ciscustcd12': 'VARCHAR',
                 'ciscustcd13': 'VARCHAR', 'ciscustcd14': 'VARCHAR', 'ciscustcd15': 'VARCHAR',
                 'consdd': 'VARCHAR', 'consmm': 'VARCHAR', 'conscc': 'VARCHAR',
                 'consyy': 'VARCHAR', 'sector_code': 'VARCHAR', 'instsect': 'VARCHAR',
                 'restatus': 'VARCHAR', 'sic_iss': 'VARCHAR', 'occupat_masco_cd': 'VARCHAR',
                 'cust_code': 'VARCHAR'}})
""")

con.execute("""
    CREATE TEMP TABLE cisln AS
    SELECT 
        *,
        custcons custconsent,
        cons_old consent_old,
        TRY_CAST(consyy || '-' || consmm || '-' || consdd AS DATE) consenteffdt
    FROM cisln_raw
    ORDER BY custno
""")

con.execute(f"""
    CREATE TEMP TABLE cisname AS
    SELECT custno, custname, addref, priphone, secphone, custnam1, mobiphon, longname
    FROM read_csv('{INPUT_DIR}/cisname.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'custname': 'VARCHAR', 'addref': 'VARCHAR',
                 'priphone': 'VARCHAR', 'secphone': 'VARCHAR', 'custnam1': 'VARCHAR',
                 'mobiphon': 'VARCHAR', 'longname': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute(f"""
    CREATE TEMP TABLE cistaxid AS
    SELECT custno, oldic, newicind, newic, bussind, bussreg, branch, rhold_ind
    FROM read_csv('{INPUT_DIR}/cistaxid.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'oldic': 'VARCHAR', 'newicind': 'VARCHAR',
                 'newic': 'VARCHAR', 'bussind': 'VARCHAR', 'bussreg': 'VARCHAR',
                 'branch': 'INTEGER', 'rhold_ind': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute(f"""
    CREATE TEMP TABLE cisbnmid_raw AS
    SELECT DISTINCT custno, bnm_assign_id
    FROM read_csv('{INPUT_DIR}/cisbnmid.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'bnm_assign_id': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute("CREATE TEMP TABLE cisbnmid AS SELECT DISTINCT * FROM cisbnmid_raw")

con.execute(f"""
    CREATE TEMP TABLE cisrmrk_raw AS
    SELECT DISTINCT custno, emailadd
    FROM read_csv('{INPUT_DIR}/cisrmrk.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'emailadd': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute("CREATE TEMP TABLE cisrmrk AS SELECT DISTINCT * FROM cisrmrk_raw")

con.execute(f"""
    CREATE TEMP TABLE cissale_raw AS
    SELECT DISTINCT custno, upd_dt_sale, TRY_CAST(turnover2 AS DOUBLE) turnover
    FROM read_csv('{INPUT_DIR}/cissale.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'upd_dt_sale': 'DATE', 'turnover2': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute("CREATE TEMP TABLE cissale AS SELECT DISTINCT * FROM cissale_raw")

con.execute(f"""
    CREATE TEMP TABLE cisempl_raw AS
    SELECT DISTINCT custno, upd_dt_employee, TRY_CAST(noemplo2 AS INTEGER) noemplo
    FROM read_csv('{INPUT_DIR}/cisempl.txt', delim='|', header=false,
        columns={{'custno': 'BIGINT', 'upd_dt_employee': 'DATE', 'noemplo2': 'VARCHAR'}})
    ORDER BY custno
""")

con.execute("CREATE TEMP TABLE cisempl AS SELECT DISTINCT * FROM cisempl_raw")

con.execute("""
    CREATE TABLE cisinfo AS
    SELECT n.*, t.oldic, t.newicind, t.newic, t.bussind, t.bussreg, t.branch, t.rhold_ind,
           b.bnm_assign_id, r.emailadd
    FROM cisname n
    LEFT JOIN cistaxid t ON n.custno = t.custno
    LEFT JOIN cisbnmid b ON n.custno = b.custno
    LEFT JOIN cisrmrk r ON n.custno = r.custno
""")

con.execute("""
    CREATE TEMP TABLE cisln_merge AS
    SELECT l.*, t.oldic, t.newicind, t.newic, t.bussind, t.bussreg, t.branch, t.rhold_ind,
           b.bnm_assign_id, n.custname, n.addref, n.priphone, n.secphone, n.custnam1,
           n.mobiphon, n.longname, r.emailadd, s.turnover, s.upd_dt_sale,
           e.noemplo, e.upd_dt_employee
    FROM cisln l
    LEFT JOIN cistaxid t ON l.custno = t.custno
    LEFT JOIN cisbnmid b ON l.custno = b.custno
    LEFT JOIN cisname n ON l.custno = n.custno
    LEFT JOIN cisrmrk r ON l.custno = r.custno
    LEFT JOIN cissale s ON l.custno = s.custno
    LEFT JOIN cisempl e ON l.custno = e.custno
    ORDER BY addref
""")

con.execute("""
    CREATE TEMP TABLE cisln_final AS
    SELECT c.*, a.addrln1, a.addrln2, a.addrln3, a.addrln4, a.addrln5,
           a.lstchgdd, a.lstchgmm, a.lstchgcc, a.lstchgyy,
           a.mailcode, a.mailstat, a.ctry, a.country
    FROM cisln_merge c
    LEFT JOIN cisaddr a ON c.addref = a.addref
""")

con.execute("CREATE TEMP TABLE cloan AS SELECT *, custno owner FROM cisln_final ORDER BY custno")

con.execute(f"""
    CREATE TEMP TABLE cisrlcc_raw AS
    SELECT 
        custxx, indorgxx, relatcd1, custyy, indorgyy, relatcd2,
        expire, expyear, expmm, expdd, expcc, expyy, bgc1, bgc2,
        TRY_CAST(expyear || '-' || expmm || '-' || expdd AS DATE) expdte
    FROM read_csv('{INPUT_DIR}/cisrlcc.txt', delim='|', header=false,
        columns={{'custxx': 'BIGINT', 'indorgxx': 'VARCHAR', 'relatcd1': 'VARCHAR',
                 'custyy': 'BIGINT', 'indorgyy': 'VARCHAR', 'relatcd2': 'VARCHAR',
                 'expire': 'VARCHAR', 'expyear': 'VARCHAR', 'expmm': 'VARCHAR',
                 'expdd': 'VARCHAR', 'expcc': 'VARCHAR', 'expyy': 'VARCHAR',
                 'bgc1': 'VARCHAR', 'bgc2': 'VARCHAR'}})
    WHERE (expdte >= '{rptdte}' OR expire = '' OR expire IS NULL)
""")

con.execute("""
    CREATE TEMP TABLE cisrl1 AS
    SELECT custxx custno, custyy owner, relatcd1, relatcd2
    FROM cisrlcc_raw
    WHERE (relatcd1 = '049' OR relatcd2 = '050' OR bgc1 IN ('21','22','23') 
           OR (relatcd1 = '050' AND relatcd2 = '058'))
      AND indorgxx = 'O' AND indorgyy = 'I'
""")

con.execute("""
    CREATE TEMP TABLE cisrl2 AS
    SELECT custyy custno, custxx owner, relatcd1, relatcd2
    FROM cisrlcc_raw
    WHERE (relatcd1 = '050' OR relatcd2 = '049' OR bgc2 IN ('21','22','23')
           OR (relatcd1 = '058' AND relatcd2 = '050'))
      AND indorgyy = 'O' AND indorgxx = 'I'
""")

con.execute("""
    CREATE TEMP TABLE cisrl3 AS
    SELECT custxx custno, custyy custno2
    FROM cisrlcc_raw
    WHERE relatcd1 = '001' AND relatcd2 = '002'
""")

con.execute("""
    CREATE TEMP TABLE cisrl4 AS
    SELECT custyy custno, custxx custno2
    FROM cisrlcc_raw
    WHERE relatcd1 = '002' AND relatcd2 = '001'
""")

con.execute("""
    CREATE TEMP TABLE cisrlcc AS
    SELECT * FROM cisrl1
    UNION ALL
    SELECT * FROM cisrl2
    ORDER BY owner
""")

con.execute("""
    CREATE TEMP TABLE owner AS
    SELECT DISTINCT
        r.custno, r.owner, r.relatcd1, r.relatcd2,
        c.custname ownernam, c.oldic ownerid1, c.newic ownerid2,
        c.lstmntdd ownerdd, c.lstmntmm ownermm, c.lstmntcc ownercc, c.lstmntyy owneryy,
        c.longname
    FROM cisrlcc r
    LEFT JOIN cloan c ON r.owner = c.owner
    WHERE c.custname IS NOT NULL AND c.custname != ''
    ORDER BY custno
""")

con.execute("CREATE TABLE cis_owner AS SELECT * FROM owner")

con.execute("""
    CREATE TEMP TABLE cisrlcc_spouse AS
    SELECT * FROM cisrl3
    UNION ALL
    SELECT * FROM cisrl4
    ORDER BY custno2
""")

con.execute("""
    CREATE TEMP TABLE cloan_spouse AS
    SELECT custno custno2, custno, acctno, custname, oldic, newic
    FROM cisln_final
    ORDER BY custno2
""")

con.execute("""
    CREATE TEMP TABLE relation_merge AS
    SELECT DISTINCT
        r.custno, r.custno2,
        c.custname spouse1, c.oldic spouse1i, c.newic spouse1d
    FROM cisrlcc_spouse r
    LEFT JOIN cloan_spouse c ON r.custno2 = c.custno2
    ORDER BY custno, custno2
""")

con.execute("""
    CREATE TEMP TABLE relation_final AS
    SELECT DISTINCT r.*, c.acctno
    FROM relation_merge r
    LEFT JOIN cloan c ON r.custno = c.custno
    ORDER BY acctno, custno
""")

con.execute("CREATE TABLE cis_relation AS SELECT * FROM relation_final")
con.execute("CREATE TABLE cis_loan AS SELECT * FROM cisln_final ORDER BY acctno, custno")

con.execute(f"COPY cisinfo TO '{OUTPUT_DIR}/cisinfo.parquet'")
con.execute(f"COPY cis_owner TO '{OUTPUT_DIR}/cis_owner.parquet'")
con.execute(f"COPY cis_relation TO '{OUTPUT_DIR}/cis_relation.parquet'")
con.execute(f"COPY cis_loan TO '{OUTPUT_DIR}/cis_loan.parquet'")

print(f"""
CIS Customer Loan Extract Complete

Output Tables:
1. CISINFO - Customer information consolidated
2. CIS_OWNER - Ownership relationships (Organization -> Individual)
3. CIS_RELATION - Spouse relationships
4. CIS_LOAN - Customer loan details with addresses

Relationship Codes:
- 001/002: Spouse relationship
- 049/050: Organization ownership
- 058: Related organization
- BGC 21/22/23: Beneficial ownership

Records:
- CISINFO: {con.execute('SELECT COUNT(*) FROM cisinfo').fetchone()[0]}
- OWNER: {con.execute('SELECT COUNT(*) FROM cis_owner').fetchone()[0]}
- RELATION: {con.execute('SELECT COUNT(*) FROM cis_relation').fetchone()[0]}
- LOAN: {con.execute('SELECT COUNT(*) FROM cis_loan').fetchone()[0]}
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
