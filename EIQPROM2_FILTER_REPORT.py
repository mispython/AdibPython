import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# Product codes
PBBPROD = [200,201,204,205,209,210,211,212,214,215,219,220,225,226,227,228,230,
           233,234,235,236,237,238,239,240,241,242,243,300,301,304,305,359,361,
           363,213,216,217,218,231,232,244,245,246,247,315,568,248,249,250,348,349,368]

PIBPROD = [152,153,154,155,423,424,425,426,175,176,177,178,400,401,402,406,407,
           408,409,410,411,412,413,414,415,416,419,420,422,429,430,464]

def calculate_report_dates(reptdate: datetime) -> dict:
    """Calculate report date variables."""
    day = reptdate.day
    
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    prevdate = reptdate.replace(day=1) - pl.duration(days=1)
    
    month = reptdate.month
    year = reptdate.year
    if month >= 6:
        pmth = month - 5
        pyear = year - 2
    else:
        pmth = month + 7
        pyear = year - 3
    
    pdate = datetime(pyear, pmth, 1).date()
    
    return {
        'nowk': nowk,
        'reptmon': f"{reptdate.month:02d}",
        'prevmon': f"{prevdate.month:02d}",
        'rdate': reptdate.strftime('%d%m%Y'),
        'reptdt': reptdate.date(),
        'prdate': pdate,
        'prevdate': prevdate.date()
    }

def process_eiqprom2_duckdb(
    reptdate: datetime,
    lnnote_pbb_path: Path,
    lnnote_pib_path: Path,
    loan_pbb_path: Path,
    loan_pib_path: Path,
    billbad_path: Path,
    lncomm_pbb_path: Path,
    lncomm_pib_path: Path,
    collater_pbb_path: Path,
    collater_pib_path: Path,
    cis_loan_path: Path,
    elds_path: Path,
    output_loan_path: Path,
    output_summary_path: Path
) -> None:
    """Process EIQPROM2 using DuckDB - 1:1 SAS conversion."""
    
    dates = calculate_report_dates(reptdate)
    print(f"Report Date: {dates['reptdt']}")
    print(f"PDATE (2.5 years back): {dates['prdate']}")
    print(f"Week: {dates['nowk']}, Month: {dates['reptmon']}")
    print("-" * 80)
    
    con = duckdb.connect(':memory:')
    
    pbb_prod_str = ','.join(map(str, PBBPROD))
    pib_prod_str = ','.join(map(str, PIBPROD))
    
    # 1. LNNOTE - Read and filter
    print("Processing LNNOTE...")
    con.execute(f"""
        CREATE TABLE lnnote AS
        SELECT 
            ACCTNO, NOTENO, NAME, FLAG1, ORGBAL, NETPROC,
            REGEXP_REPLACE(REGEXP_REPLACE(SUBSTR(TRIM(VINNO), 1, 13), 
                          '[@*(#)\\-]', '', 'g'), ' ', '', 'g') AS AANO,
            COLLMAKE, COLLYEAR, NTINDEX, SPREAD, MODELDES, SCORE1,
            IA_LRU, BORSTAT, DELQCD, GUAREND, MAILCODE
        FROM (
            SELECT * FROM read_parquet('{lnnote_pbb_path}')
            UNION ALL
            SELECT * FROM read_parquet('{lnnote_pib_path}')
        )
        WHERE (LOANTYPE IN ({pbb_prod_str}) OR LOANTYPE IN ({pib_prod_str}))
          AND LOANSTAT != 3
          AND CURBAL > 0
          AND LSTTRNCD != 661
          AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTR(TRIM(VINNO), 1, 13), 
                                   '[@*(#)\\-]', '', 'g'), ' ', '', 'g')) = 13
          AND COLLMAKE IN ('.', '0', '')
          AND (COLLYEAR IS NULL OR COLLYEAR = 0)
    """)
    
    count = con.execute("SELECT COUNT(*) FROM lnnote").fetchone()[0]
    print(f"LNNOTE records: {count:,}")
    
    # 2. LOAN - Read and process
    print("Processing LOAN...")
    con.execute(f"""
        CREATE TABLE loan AS
        SELECT 
            ACCTNO, NOTENO, COMMNO, ISSDTE, PRODUCT, BRANCH, 
            APPRLIMT, BALANCE, EXPRDATE,
            CASE 
                WHEN BLDATE > 0 THEN DATEDIFF('day', BLDATE, DATE '{dates['reptdt']}')
                ELSE 0 
            END AS DAYDIFF,
            CASE 
                WHEN EXPRDATE IS NOT NULL THEN YEAR(EXPRDATE) - {dates['reptdt'].year}
                ELSE 0 
            END AS REMTERM,
            SUBSTR(LPAD(CAST(NOTENO AS VARCHAR), 5, '0'), 1, 1) AS NOTE1,
            SUBSTR(LPAD(CAST(NOTENO AS VARCHAR), 5, '0'), 2, 1) AS NOTE2
        FROM (
            SELECT * FROM read_parquet('{loan_pbb_path}')
            UNION ALL
            SELECT * FROM read_parquet('{loan_pib_path}')
        )
        WHERE (PRODUCT IN ({pbb_prod_str}) OR PRODUCT IN ({pib_prod_str}))
          AND LOANSTAT != 3
          AND CURBAL > 0
    """)
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"LOAN records: {count:,}")
    
    # 3. Sort and remove BILLBAD
    print("Sorting and removing BILLBAD...")
    con.execute(f"""
        CREATE TABLE billbad AS
        SELECT DISTINCT ACCTNO, NOTENO 
        FROM read_parquet('{billbad_path}')
    """)
    
    con.execute("""
        CREATE TABLE loan_sorted AS
        SELECT * FROM loan ORDER BY ACCTNO, NOTENO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_sorted RENAME TO loan")
    
    con.execute("""
        CREATE TABLE lnnote_sorted AS
        SELECT * FROM lnnote ORDER BY ACCTNO, NOTENO
    """)
    con.execute("DROP TABLE lnnote")
    con.execute("ALTER TABLE lnnote_sorted RENAME TO lnnote")
    
    # Merge and exclude billbad (SAS: MERGE with IN=A, IF A THEN DELETE)
    con.execute("""
        CREATE TABLE loan_merged AS
        SELECT n.*, l.COMMNO, l.ISSDTE, l.DAYDIFF, l.PRODUCT, 
               l.NOTE1, l.NOTE2, l.BRANCH, l.APPRLIMT, l.BALANCE, 
               l.EXPRDATE, l.REMTERM
        FROM lnnote n
        INNER JOIN loan l ON n.ACCTNO = l.ACCTNO AND n.NOTENO = l.NOTENO
        LEFT JOIN billbad b ON n.ACCTNO = b.ACCTNO AND n.NOTENO = b.NOTENO
        WHERE b.ACCTNO IS NULL
    """)
    
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_merged RENAME TO loan")
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After removing BILLBAD: {count:,}")
    
    # 4. Sort by ACCTNO, COMMNO and merge LNCOMM
    print("Merging LNCOMM...")
    con.execute("""
        CREATE TABLE loan_sorted AS
        SELECT * FROM loan ORDER BY ACCTNO, COMMNO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_sorted RENAME TO loan")
    
    con.execute(f"""
        CREATE TABLE lncomm AS
        SELECT DISTINCT ACCTNO, COMMNO, CORGAMT
        FROM (
            SELECT ACCTNO, COMMNO, CORGAMT FROM read_parquet('{lncomm_pbb_path}')
            UNION ALL
            SELECT ACCTNO, COMMNO, CORGAMT FROM read_parquet('{lncomm_pib_path}')
        )
        ORDER BY ACCTNO, COMMNO
    """)
    
    con.execute("""
        CREATE TABLE loan_with_comm AS
        SELECT l.*, c.CORGAMT
        FROM loan l
        LEFT JOIN lncomm c ON l.ACCTNO = c.ACCTNO AND l.COMMNO = c.COMMNO
    """)
    
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_with_comm RENAME TO loan")
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After merging LNCOMM: {count:,}")
    
    # 5. Apply main filters and calculate LMTAPPR, REPAID
    print("Applying main filters...")
    con.execute(f"""
        CREATE TABLE loan_filtered AS
        SELECT *,
               GREATEST(
                   COALESCE(CORGAMT, 0), 
                   COALESCE(ORGBAL, 0), 
                   COALESCE(NETPROC, 0), 
                   COALESCE(APPRLIMT, 0)
               ) AS LMTAPPR
        FROM loan
        WHERE BALANCE > 30000
          AND ISSDTE < DATE '{dates['prdate']}'
          AND FLAG1 = 'F'
          AND DAYDIFF <= 0
          AND (PRODUCT IN ({pbb_prod_str}) OR PRODUCT IN ({pib_prod_str}))
          AND NOTE1 != '1'
          AND NOTE2 != '2'
    """)
    
    con.execute("DROP TABLE loan")
    con.execute("""
        CREATE TABLE loan AS
        SELECT *, (LMTAPPR - BALANCE) AS REPAID
        FROM loan_filtered
    """)
    con.execute("DROP TABLE loan_filtered")
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After main filters: {count:,}")
    
    # 6. Exclude based on risk indicators
    print("Applying exclusion rules...")
    con.execute("""
        DELETE FROM loan
        WHERE (NTINDEX = 1 AND SPREAD = 0.00)
           OR (NTINDEX = 1 AND SPREAD = 3.50)
           OR (NTINDEX = 38 AND SPREAD = 3.20)
           OR (NTINDEX = 38 AND SPREAD = 6.70)
           OR SUBSTR(MODELDES, 1, 1) IN ('S','T','R','C')
           OR MODELDES = 'Z'
           OR SUBSTR(MODELDES, 5, 1) = 'F'
           OR SUBSTR(SCORE1, 1, 1) IN ('D','E','F','G','H','I')
           OR IA_LRU = 'I'
           OR BORSTAT = 'K'
           OR DELQCD IN ('9','09','10','11','12','13','14','15','16','17','18','19','20')
    """)
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After exclusion rules: {count:,}")
    
    # 7. Sort and process COLLATER
    print("Processing COLLATER...")
    con.execute("""
        CREATE TABLE loan_sorted AS
        SELECT * FROM loan ORDER BY ACCTNO, NOTENO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_sorted RENAME TO loan")
    
    con.execute(f"""
        CREATE TABLE coll_temp AS
        SELECT 
            ACCTNO, NOTENO, CPRPROPD, MRESERVE, CPRLANDU, HOLDEXPD,
            CASE 
                WHEN EXPDATE > 0 THEN CAST(EXPDATE AS DATE)
                ELSE NULL 
            END AS EXPDT,
            CASE 
                WHEN (
                    (CPRPROPD IN ('10','11','32','33','34','35') AND
                     CPRLANDU IN ('10','11','32','33','34','35'))
                    OR MRESERVE = 'Y'
                ) THEN 'Y' 
                ELSE NULL 
            END AS EXCL
        FROM (
            SELECT * FROM read_parquet('{collater_pbb_path}')
            UNION ALL
            SELECT * FROM read_parquet('{collater_pib_path}')
        )
    """)
    
    con.execute("""
        CREATE TABLE coll AS
        SELECT * FROM coll_temp
        WHERE EXCL = 'Y' OR HOLDEXPD = 'L'
        ORDER BY ACCTNO, NOTENO
    """)
    con.execute("DROP TABLE coll_temp")
    
    con.execute("""
        CREATE TABLE loan_with_coll AS
        SELECT l.*, c.EXCL, c.HOLDEXPD AS COLL_HOLDEXPD, c.EXPDT
        FROM loan l
        LEFT JOIN coll c ON l.ACCTNO = c.ACCTNO AND l.NOTENO = c.NOTENO
    """)
    
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_with_coll RENAME TO loan")
    
    con.execute("""
        DELETE FROM loan
        WHERE EXCL = 'Y'
           OR (
               COLL_HOLDEXPD = 'L' 
               AND EXPDT IS NOT NULL 
               AND EXPRDATE IS NOT NULL
               AND (DATEDIFF('day', EXPDT, EXPRDATE) / 365.0) < 30
           )
    """)
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After COLLATER exclusion: {count:,}")
    
    # 8. CIS exclusions
    print("Processing CIS exclusions...")
    con.execute(f"""
        CREATE TABLE cis AS
        SELECT ACCTNO, CUSTNO, SECCUST
        FROM read_parquet('{cis_loan_path}')
    """)
    
    con.execute("""
        CREATE TABLE cis_sorted AS
        SELECT * FROM cis ORDER BY CUSTNO
    """)
    con.execute("DROP TABLE cis")
    con.execute("ALTER TABLE cis_sorted RENAME TO cis")
    
    con.execute("""
        CREATE TABLE cisln_temp AS
        SELECT ACCTNO, CUSTNO
        FROM cis
        WHERE NOT (
            (ACCTNO BETWEEN 2500000000 AND 2599999999)
            OR (ACCTNO BETWEEN 2850000000 AND 2859999999)
        )
    """)
    
    con.execute("""
        CREATE TABLE cisbill AS
        SELECT DISTINCT CUSTNO
        FROM cis
        WHERE (ACCTNO BETWEEN 2500000000 AND 2599999999)
           OR (ACCTNO BETWEEN 2850000000 AND 2859999999)
    """)
    
    con.execute("""
        CREATE TABLE cisln AS
        SELECT DISTINCT c.ACCTNO
        FROM cisln_temp c
        INNER JOIN cisbill b ON c.CUSTNO = b.CUSTNO
    """)
    con.execute("DROP TABLE cisln_temp")
    
    con.execute("""
        CREATE TABLE cis_901 AS
        SELECT DISTINCT ACCTNO
        FROM cis
        WHERE SECCUST = '901'
        ORDER BY ACCTNO
    """)
    
    con.execute("""
        DELETE FROM loan
        WHERE ACCTNO IN (SELECT ACCTNO FROM cisln)
           OR ACCTNO IN (SELECT ACCTNO FROM cis_901)
    """)
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After CIS exclusion: {count:,}")
    
    # 9. Sort by AANO and ELDS exclusion
    print("Processing ELDS exclusion...")
    con.execute("""
        CREATE TABLE loan_sorted AS
        SELECT * FROM loan ORDER BY AANO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_sorted RENAME TO loan")
    
    con.execute(f"""
        CREATE TABLE elds AS
        SELECT AANO, REINPROD
        FROM read_parquet('{elds_path}')
        ORDER BY AANO
    """)
    
    con.execute("""
        CREATE TABLE loan_with_elds AS
        SELECT l.*, e.REINPROD
        FROM loan l
        LEFT JOIN elds e ON l.AANO = e.AANO
    """)
    
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_with_elds RENAME TO loan")
    
    con.execute("""
        DELETE FROM loan
        WHERE REINPROD = 'Y'
    """)
    
    count = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    print(f"After ELDS exclusion: {count:,}")
    
    # 10. Add NEW flag
    print("Adding NEW flag...")
    con.execute("""
        CREATE TABLE loan_sorted AS
        SELECT * FROM loan ORDER BY ACCTNO, NOTENO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_sorted RENAME TO loan")
    
    prev_path = output_loan_path.parent / f"LOAN{dates['prevmon']}.parquet"
    if prev_path.exists():
        con.execute(f"""
            CREATE TABLE prev_loan AS
            SELECT DISTINCT ACCTNO, NOTENO 
            FROM read_parquet('{prev_path}')
        """)
        
        con.execute("""
            CREATE TABLE loan_with_new AS
            SELECT l.*,
                   CASE WHEN p.ACCTNO IS NULL THEN 'Y' ELSE '' END AS NEW
            FROM loan l
            LEFT JOIN prev_loan p ON l.ACCTNO = p.ACCTNO AND l.NOTENO = p.NOTENO
        """)
        
        con.execute("DROP TABLE loan")
        con.execute("ALTER TABLE loan_with_new RENAME TO loan")
    else:
        con.execute("ALTER TABLE loan ADD COLUMN NEW VARCHAR")
        con.execute("UPDATE loan SET NEW = 'Y'")
    
    # 11. Final sort and save
    print("Saving output...")
    con.execute("""
        CREATE TABLE loan_final AS
        SELECT * FROM loan ORDER BY BRANCH, ACCTNO
    """)
    con.execute("DROP TABLE loan")
    con.execute("ALTER TABLE loan_final RENAME TO loan")
    
    result = con.execute("SELECT * FROM loan").pl()
    result.write_parquet(output_loan_path)
    
    print(f"\nFinal output: {len(result):,} records")
    print(f"Saved to: {output_loan_path}")
    
    # 12. Create summary (PROC SUMMARY equivalent)
    summary = con.execute("""
        SELECT 
            BRANCH AS BRCH,
            COUNT(*) AS NOACCT,
            SUM(LMTAPPR) AS LMTAPPR,
            SUM(BALANCE) AS BALANCE,
            SUM(REPAID) AS REPAID
        FROM loan
        GROUP BY BRANCH
        ORDER BY BRANCH
    """).pl()
    
    summary.write_parquet(output_summary_path)
    print(f"Summary saved to: {output_summary_path}")
    
    print("\n" + "="*80)
    print("SUMMARY BY BRANCH")
    print("="*80)
    print(summary)
    
    con.close()

if __name__ == "__main__":
    report_date = datetime.now()
    input_dir = Path(".")
    
    process_eiqprom2_duckdb(
        reptdate=report_date,
        lnnote_pbb_path=input_dir / "PBBLN_LNNOTE.parquet",
        lnnote_pib_path=input_dir / "PIBLN_LNNOTE.parquet",
        loan_pbb_path=input_dir / f"PBBSAS_LOAN{report_date.month:02d}.parquet",
        loan_pib_path=input_dir / f"PIBSAS_LOAN{report_date.month:02d}.parquet",
        billbad_path=input_dir / "LNBILL_BILL.parquet",
        lncomm_pbb_path=input_dir / "PBBLN_LNCOMM.parquet",
        lncomm_pib_path=input_dir / "PIBLN_LNCOMM.parquet",
        collater_pbb_path=input_dir / "COLL_COLLATER.parquet",
        collater_pib_path=input_dir / "COLLI_COLLATER.parquet",
        cis_loan_path=input_dir / "CIS_LOAN.parquet",
        elds_path=input_dir / "ELDS_ELBNMAX.parquet",
        output_loan_path=input_dir / f"LOAN{report_date.month:02d}.parquet",
        output_summary_path=input_dir / "BILLSUM.parquet"
    )
