#!/usr/bin/env python3
"""
EIFMNP02 - NPL Account Preparation
BORSTAT: F=Deficit, I=Irregular, R=Repossess, T=Insurance, S=Restructure, 
         W=Written-off, Z=Prior Yr W/O, Y=Doubtful, A=Reinstated
"""

import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
BASE_INPUT_PATH = Path("Data_Warehouse/NPL/input")
NPL_DATA_PATH = Path("Data_Warehouse/NPL/output")
REPTDATE = datetime.today() - timedelta(days=1)
REPTMON = REPTDATE.strftime('%m')
OUTPUT_DATA_PATH = Path(f"{NPL_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

print(f"EIFMNP02 - NPL Processing for {REPTDATE.strftime('%Y-%m-%d')}, Month: {REPTMON}")

# Read inputs
LNNOTE = pl.read_parquet(BASE_INPUT_PATH / "LOAN" / "lnnote.parquet")
REPTDATE_DF = pl.read_parquet(BASE_INPUT_PATH / "LOAN" / "reptdate.parquet")
NPLOBAL = pl.read_parquet(BASE_INPUT_PATH / "NPL6" / "nplobal.parquet")
WIIS = pl.read_parquet(BASE_INPUT_PATH / "NPL6" / "wiis.parquet")
WAQ = pl.read_parquet(BASE_INPUT_PATH / "NPL6" / "waq.parquet")
WSP2 = pl.read_parquet(BASE_INPUT_PATH / "NPL6" / "wsp2.parquet")

# Initialize DuckDB
con = duckdb.connect(':memory:')
for name, df in [('lnnote', LNNOTE), ('reptdate_df', REPTDATE_DF), ('nplobal', NPLOBAL), 
                 ('wiis', WIIS), ('waq', WAQ), ('wsp2', WSP2)]:
    con.register(name, df)

# PROC SORT DATA=LOAN.LNNOTE OUT=LOAN
LOAN = con.execute("""
    SELECT * FROM lnnote
    WHERE (reversed IS NULL OR reversed != 'Y') AND noteno IS NOT NULL 
      AND (paidind IS NULL OR paidind != 'P') AND ntbrch IS NOT NULL
      AND loantype IN (128,130,380,381,700,705,131,132,720,725)
    ORDER BY acctno, noteno
""").pl()
con.register('loan', LOAN)

# PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE (DROP=NFEEAMT10 NFEEAMT11 NFEEAMT12); PROC SORT NODUPKEY
LNNOTE_DEDUP = con.execute("""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY acctno ORDER BY noteno DESC) as rn
        FROM lnnote
        WHERE (reversed IS NULL OR reversed != 'Y') AND noteno IS NOT NULL 
          AND ntbrch IS NOT NULL
          AND loantype IN (128,130,700,705,983,993,996,380,381,131,132,720,725)
    ) WHERE rn = 1
""").pl()
con.register('lnnote_dedup', LNNOTE_DEDUP)

# PROC SORT DATA=LOAN.LNNOTE OUT=LOANNO; PROC SORT NODUPKEY
LOANNO = con.execute("""
    SELECT * FROM (
        SELECT acctno, noteno, ROW_NUMBER() OVER (PARTITION BY acctno ORDER BY noteno DESC) as rn
        FROM lnnote
        WHERE (reversed IS NULL OR reversed != 'Y') AND noteno IS NOT NULL 
          AND (paidind IS NULL OR paidind != 'P') AND ntbrch IS NOT NULL
          AND loantype IN (128,130,380,381,700,705,983,993,996,131,132,720,725)
    ) WHERE rn = 1
""").pl()
con.register('loanno', LOANNO)

# DATA LNNOTE; MERGE LNNOTE(IN=A) LOANNO; BY ACCTNO; IF A
LNNOTE_MERGED = con.execute("""
    SELECT l.* FROM lnnote_dedup l INNER JOIN loanno n ON l.acctno = n.acctno
""").pl()
con.register('lnnote_merged', LNNOTE_MERGED)

# Update NPL6 datasets
for tbl in ['wiis', 'waq', 'wsp2']:
    con.execute(f"""
        CREATE OR REPLACE TABLE {tbl}_updated AS
        SELECT w.*, COALESCE(l.noteno, w.noteno) as noteno
        FROM {tbl} w LEFT JOIN lnnote_merged l ON w.acctno = l.acctno
    """)

# DATA NPLOBAL6
NPLOBAL6 = con.execute("""
    SELECT 
        COALESCE(n.acctno, l.acctno, w.acctno) as acctno,
        COALESCE(l.noteno, n.noteno) as noteno,
        n.ntbrch, n.name, n.loantype, n.loanstat, n.iisp, n.oip, n.spp1, n.spp2, 
        n.curbalp, n.netbalp, n.exist, n.uhcp,
        CASE WHEN w.acctno IS NOT NULL THEN 'W' ELSE n.borstat END as borstat,
        l.* EXCLUDE (acctno, noteno, loanstat, borstat)
    FROM nplobal n
    FULL OUTER JOIN lnnote_merged l ON n.acctno = l.acctno
    LEFT JOIN wiis_updated w ON n.acctno = w.acctno
    WHERE n.acctno IS NOT NULL OR w.acctno IS NOT NULL
""").pl()
con.register('nplobal6', NPLOBAL6)

# Main NPL Classification
LOAN_REPTMON = con.execute(f"""
    WITH base AS (
        SELECT 
            n.*, l.* EXCLUDE (acctno, noteno, loanstat, borstat), r.reptdate,
            TRY_CAST(CAST(l.issuedt AS VARCHAR) AS DATE) as issdte,
            SUBSTRING(LPAD(CAST(l.census AS VARCHAR), 8, '0'), 7, 1) as census7,
            CASE WHEN n.acctno = 8095419311 THEN DATE '1997-01-21' ELSE l.bldate END as bldate_calc,
            CASE WHEN COALESCE(l.intamt,0) > 0.1 THEN COALESCE(l.intamt,0) + COALESCE(l.intearn2,0) ELSE 0 END as termchg,
            CASE WHEN n.acctno IS NOT NULL THEN TRUE ELSE FALSE END as in_nplobal6,
            CASE WHEN l.acctno IS NOT NULL THEN TRUE ELSE FALSE END as in_loan
        FROM nplobal6 n
        FULL OUTER JOIN loan l ON n.acctno = l.acctno
        CROSS JOIN reptdate_df r
        WHERE COALESCE(n.borstat, '') != 'Z'
    ),
    calc AS (
        SELECT *,
            CASE 
                WHEN bldate_calc IS NOT NULL AND bldate_calc > DATE '1900-01-01' THEN
                    CASE WHEN bilpay > 0 AND (COALESCE(biltot,0)/bilpay) <= 0.01 
                         THEN CAST(reptdate AS DATE) - (bldate_calc + INTERVAL '1 month')
                         ELSE CAST(reptdate AS DATE) - bldate_calc END
                ELSE 0 END as days_calc,
            CASE WHEN COALESCE(borstat,'') NOT IN ('F','I','R','Y','W','Z','A') 
                      AND noteno IN (98010,98011,99010,99011) THEN 'S' ELSE borstat END as borstat_upd
        FROM base
    )
    SELECT 
        loantype, ntbrch, acctno, noteno, name, 
        CASE WHEN in_nplobal6 AND NOT in_loan THEN ''
             WHEN loanstat = 1 AND COALESCE(borstat_upd,'') = '' THEN 'A'
             ELSE borstat_upd END as borstat,
        CASE WHEN in_nplobal6 THEN
                CASE WHEN NOT in_loan THEN NULL WHEN days_calc <= 0 THEN 1 ELSE loanstat END
             ELSE loanstat END as loanstat,
        netproc, orgbal, curbal, noteterm, issdte, bldate_calc as bldate, bilpay, intrate,
        appvalue, marketvl, 
        CASE WHEN in_nplobal6 AND NOT in_loan THEN 0
             WHEN COALESCE(oldnotedayarr,0) > 0 AND noteno BETWEEN 98000 AND 98999 
                  THEN GREATEST(0, days_calc) + oldnotedayarr
             ELSE days_calc END as days,
        termchg, iisp, oip, spp1, spp2, curbalp, netbalp, user5,
        feeamt, feeamt3, feeamt4, exist, feeytd, feepdytd, uhcp, census7,
        vinno, feetot2, feeamt8, pendbrh, costctr, earnterm, feeamta, feeamt5,
        oldnotedayarr, accrual, in_nplobal6, in_loan
    FROM calc
""").pl()
con.register('loan_reptmon', LOAN_REPTMON)

# Split LOAN and PLOAN
PLOAN_REPTMON = con.execute("""
    SELECT * EXCLUDE (in_nplobal6, in_loan) FROM loan_reptmon
    WHERE (NOT in_nplobal6 AND NOT (days > 89 OR COALESCE(borstat,'') NOT IN ('','A','C','S','T') OR user5 = 'N'))
       OR (NOT in_nplobal6 AND borstat = 'Y' AND days <= 182)
""").pl()

LOAN_REPTMON_FINAL = con.execute("""
    SELECT * EXCLUDE (in_nplobal6, in_loan) FROM loan_reptmon
    WHERE in_nplobal6 
       OR (NOT in_nplobal6 AND (days > 89 OR COALESCE(borstat,'') NOT IN ('','A','C','S','T') OR user5 = 'N')
           AND NOT (borstat = 'Y' AND days <= 182))
""").pl()

# Export
LOAN_REPTMON_FINAL.write_parquet(OUTPUT_DATA_PATH / f"LOAN{REPTMON}.parquet", compression='snappy')
LOAN_REPTMON_FINAL.write_csv(OUTPUT_DATA_PATH / f"LOAN{REPTMON}.csv")
PLOAN_REPTMON.write_parquet(OUTPUT_DATA_PATH / f"PLOAN{REPTMON}.parquet", compression='snappy')
PLOAN_REPTMON.write_csv(OUTPUT_DATA_PATH / f"PLOAN{REPTMON}.csv")

# Summary
print(f"\n{'='*70}")
print("SUMMARY REPORT")
print(f"{'='*70}")
print(f"NPL Accounts (LOAN{REPTMON}): {LOAN_REPTMON_FINAL.height:,} records, Balance: ${LOAN_REPTMON_FINAL['curbal'].sum():,.2f}")
print(f"Performing Loans (PLOAN{REPTMON}): {PLOAN_REPTMON.height:,} records, Balance: ${PLOAN_REPTMON['curbal'].sum():,.2f}")

# BORSTAT breakdown
for row in LOAN_REPTMON_FINAL.group_by('borstat').agg([
    pl.count('acctno').alias('count'), pl.sum('curbal').alias('balance')
]).sort('borstat').iter_rows(named=True):
    print(f"  BORSTAT {row['borstat'] or '(blank)':5}: {row['count']:6,} accounts, ${row['balance']:,.2f}")

con.close()
print(f"\n✓ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
