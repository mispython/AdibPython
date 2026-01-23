import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION - Edit these paths to match your environment
# ============================================================================

# Base directories
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input paths
LOAN_REPTDATE = INPUT_DIR / 'loan' / 'reptdate.parquet'
CRFTABL = INPUT_DIR / 'crftabl.txt'
BTRSA_ICOMM = INPUT_DIR / 'btrsa' / 'icomm{reptday}{reptmon}.parquet'
BTRSA_IHSTR = INPUT_DIR / 'btrsa' / 'ihstr{reptday}{reptmon}.parquet'
BTRSA_IPROV = INPUT_DIR / 'btrsa' / 'iprov{reptday}{reptmon}.parquet'
BTRSA_ICRED = INPUT_DIR / 'btrsa' / 'icred{reptday}{reptmon}.parquet'
BTRSA_ISUBA = INPUT_DIR / 'btrsa' / 'isuba{reptday}{reptmon}.parquet'
BTRSA_IMAST = INPUT_DIR / 'btrsa' / 'imast{reptday}{reptmon}.parquet'
BTRSA_IAVDB = INPUT_DIR / 'btrsa' / 'iavdb{reptday}{reptmon}.parquet'
BTRWH1_IEXTCOM_PREV = INPUT_DIR / 'btrwh1' / 'iextcom{reptmon1}{nowks}.parquet'

# Output paths
BTRWH1_IEXTCOM = OUTPUT_DIR / 'btrwh1' / 'iextcom{reptmon}{nowk}.parquet'
BTRWH1_IBTDTL = OUTPUT_DIR / 'btrwh1' / 'ibtdtl{reptmon}{nowk}.parquet'
REPORT_TXT = OUTPUT_DIR / 'btrade_summary_{rdate}.txt'

# Create output directories
(OUTPUT_DIR / 'btrwh1').mkdir(parents=True, exist_ok=True)

# ============================================================================

# Initialize DuckDB connection
con = duckdb.connect()

# ============================================================================
# Get REPTDATE and derive variables
# ============================================================================

con.execute(f"CREATE TEMP TABLE reptdate_tbl AS SELECT * FROM read_parquet('{LOAN_REPTDATE}')")
reptdate_row = con.execute("SELECT * FROM reptdate_tbl").fetchone()
reptdate = reptdate_row[0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Determine week indicators
if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'
    wk2, wk3 = '2', '1'

# Calculate MM1
mm1 = mm - 1 if mm >= 2 else 12

# Format variables
nowk = wk
nowks = '4'
nowk1 = wk1
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(year)
reptday = f"{day:02d}"
rdate = reptdate.strftime('%d%m%Y')
sdate = datetime(year, mm, sdd)
startdt = datetime(year, mm, 1)

print(f"Processing date: {reptdate}")
print(f"Week: {nowk}, Month: {reptmon}, Year: {reptyear}")
print(f"Start date: {startdt}, SDate: {sdate}")

# Build paths with variables
icomm_path = str(BTRSA_ICOMM).format(reptday=reptday, reptmon=reptmon)
ihstr_path = str(BTRSA_IHSTR).format(reptday=reptday, reptmon=reptmon)
iprov_path = str(BTRSA_IPROV).format(reptday=reptday, reptmon=reptmon)
icred_path = str(BTRSA_ICRED).format(reptday=reptday, reptmon=reptmon)
isuba_path = str(BTRSA_ISUBA).format(reptday=reptday, reptmon=reptmon)
imast_path = str(BTRSA_IMAST).format(reptday=reptday, reptmon=reptmon)
iavdb_path = str(BTRSA_IAVDB).format(reptday=reptday, reptmon=reptmon)
iextcom_prev_path = str(BTRWH1_IEXTCOM_PREV).format(reptmon1=reptmon1, nowks=nowks)
output_iextcom = str(BTRWH1_IEXTCOM).format(reptmon=reptmon, nowk=nowk)
output_ibtdtl = str(BTRWH1_IBTDTL).format(reptmon=reptmon, nowk=nowk)
output_report = str(REPORT_TXT).format(rdate=rdate)

# ============================================================================
# Load TABLE from CRFTABL fixed-width file
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE table_data AS
    SELECT 
        SUBSTRING(line, 1, 3) AS tfnum,
        SUBSTRING(line, 4, 8) AS tfid,
        SUBSTRING(line, 12, 5) AS subacct,
        SUBSTRING(line, 365, 1) AS tfindr02,
        SUBSTRING(line, 366, 1) AS tfindr03,
        SUBSTRING(line, 368, 1) AS tfindr05,
        SUBSTRING(line, 377, 10) AS acctno
    FROM read_csv('{CRFTABL}', columns={{'line': 'VARCHAR'}}, header=false)
""")

con.execute("CREATE TEMP TABLE table_tbl AS SELECT DISTINCT * FROM table_data ORDER BY acctno, subacct")

# ============================================================================
# Process PRNGL from ICOMM
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE prngl_base AS
    SELECT *,
           glmnemo AS glmnem,
           CASE WHEN creatds NOT IN (0, NULL) 
                THEN TRY_STRPTIME(LPAD(CAST(creatds AS VARCHAR), 6, '0'), '%y%m%d')
                ELSE NULL END AS issdte,
           CASE WHEN expirds NOT IN (0, NULL)
                THEN TRY_STRPTIME(LPAD(CAST(expirds AS VARCHAR), 6, '0'), '%y%m%d')
                ELSE NULL END AS exprdate,
           CASE WHEN paygrno != ' ' THEN SUBSTRING(paygrno, 1, 7) ELSE NULL END AS transref
    FROM read_parquet('{icomm_path}')
    WHERE glmnemo NOT IN ('CONI', 'CONM', 'CONP', 'DDA', 'HOE', 'IBS')
      AND SUBSTRING(glmnemo, 1, 1) != '9'
      AND issdte <= DATE '{reptdate}'
""")

# Adjust GLMNEMO
con.execute("""
    CREATE TEMP TABLE prngl AS
    SELECT *,
           CASE WHEN SUBSTRING(glmnem, 1, 1) = '0' THEN glmnem || '0'
                ELSE glmnem END AS glmnemo_adj
    FROM prngl_base
    ORDER BY transref
""")

# ============================================================================
# Process EXTHSTR from IHSTR
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE exthstr AS
    SELECT DISTINCT transref, acctno, subacct, agent1, agent2
    FROM read_parquet('{ihstr_path}')
    ORDER BY transref, acctno
""")

con.execute("CREATE TEMP TABLE exthstr_unique AS SELECT DISTINCT * FROM exthstr ORDER BY transref")

# ============================================================================
# Merge and create EXTCOMM
# ============================================================================

con.execute("""
    CREATE TEMP TABLE extcomm AS
    SELECT p.*, e.agent1, e.agent2
    FROM prngl p
    LEFT JOIN exthstr_unique e ON p.transref = e.transref
    ORDER BY branch, glmnemo_adj, acctno, subacct, liabcode, transref
""")

# Aggregate EXTCOMM
con.execute("""
    CREATE TEMP TABLE extcomm2_initial AS
    SELECT branch, glmnemo_adj as glmnemo, acctno, subacct, liabcode, transref,
           SUM(glmneamt) as gltrntot
    FROM extcomm
    GROUP BY branch, glmnemo_adj, acctno, subacct, liabcode, transref
""")

# Branch code remapping
branch_map = {
    119: 207, 132: 197, 182: 58, 212: 165, 213: 57, 227: 81,
    229: 151, 255: 68, 223: 24, 236: 69, 246: 146, 250: 92
}

branch_remap = ', '.join([f"WHEN {k} THEN {v}" for k, v in branch_map.items()])

con.execute(f"""
    CREATE TEMP TABLE extcomm2_remap AS
    SELECT 
        CASE {branch_remap} ELSE branch END as branch,
        glmnemo, acctno, subacct, liabcode, transref, gltrntot
    FROM extcomm2_initial
    
    UNION ALL
    
    SELECT 
        CASE {branch_remap} ELSE branch END as branch,
        glmnemo, acctno, subacct, liabcode, transref, gltrntot
    FROM read_parquet('{iextcom_prev_path}')
""")

# Re-aggregate after adding previous period
con.execute("""
    CREATE TEMP TABLE extcomm2 AS
    SELECT branch, glmnemo, acctno, subacct, liabcode, transref,
           SUM(gltrntot) as gltrntot
    FROM extcomm2_remap
    GROUP BY branch, glmnemo, acctno, subacct, liabcode, transref
""")

# Save IEXTCOM output
con.execute(f"COPY extcomm2 TO '{output_iextcom}'")

# ============================================================================
# Calculate VOLUME
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE volumn AS
    SELECT branch, glmnemo_adj as glmnemo, acctno, subacct, liabcode, transref,
           COUNT(*) as novolumn,
           SUM(glmneamt) as volumn
    FROM extcomm
    WHERE glmneamt > 0 
      AND issdte BETWEEN DATE '{startdt}' AND DATE '{reptdate}'
    GROUP BY branch, glmnemo_adj, acctno, subacct, liabcode, transref
""")

# Merge volume into extcomm2
con.execute("""
    CREATE TEMP TABLE extcomm2_vol AS
    SELECT e.*, v.novolumn, v.volumn
    FROM extcomm2 e
    LEFT JOIN volumn v ON e.branch = v.branch 
                       AND e.glmnemo = v.glmnemo
                       AND e.acctno = v.acctno
                       AND e.subacct = v.subacct
                       AND e.liabcode = v.liabcode
                       AND e.transref = v.transref
""")

# ============================================================================
# Add TRANOLD (issue/expiry dates)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE tranold AS
    SELECT DISTINCT branch, glmnemo_adj as glmnemo, liabcode, transref, 
           issdte, exprdate
    FROM prngl
    ORDER BY branch, glmnemo_adj, liabcode, transref
""")

con.execute("""
    CREATE TEMP TABLE extcomm2_dated AS
    SELECT e.*, t.issdte, t.exprdate,
           e.subacct as subacct1
    FROM extcomm2_vol e
    LEFT JOIN tranold t ON e.branch = t.branch
                        AND e.glmnemo = t.glmnemo
                        AND e.liabcode = t.liabcode
                        AND e.transref = t.transref
    ORDER BY acctno, transref
""")

# ============================================================================
# Process PROV (provisions)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE prov AS
    SELECT DISTINCT acctno, transrex as transref
    FROM read_parquet('{iprov_path}')
    ORDER BY acctno, transref
""")

# ============================================================================
# Process CRED (credit data)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE cred AS
    SELECT acctno,
           SUBSTRING(transref, 1, 7) as transref,
           prinamt, intamt, iisamt, totiisr, writoff, outstand, 
           avgdbal, nplind, maturedx
    FROM read_parquet('{icred_path}')
""")

con.execute("CREATE TEMP TABLE cred_unique AS SELECT DISTINCT * FROM cred ORDER BY acctno, transref")

# Merge CRED and PROV
con.execute("""
    CREATE TEMP TABLE credprov AS
    SELECT c.acctno, c.transref, c.prinamt, c.intamt, c.iisamt, 
           c.totiisr, c.writoff, c.outstand, c.avgdbal, c.nplind, c.maturedx
    FROM cred_unique c
    LEFT JOIN prov p ON c.acctno = p.acctno AND c.transref = p.transref
""")

# ============================================================================
# Process SUBACCT from ISUBA and IMAST
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE subacct_base AS
    SELECT s.*,
           m.tfid,
           SUBSTRING(s.transref, 1, 7) as transref_adj,
           s.limtcurm as apprlimt,
           s.limtcurf as apprlim2,
           CASE WHEN s.ccrisfac = '00000' THEN '       ' ELSE SUBSTRING(s.transref, 1, 7) END as transref_final
    FROM read_parquet('{isuba_path}') s
    LEFT JOIN read_parquet('{imast_path}') m ON s.acctno = m.acctno
""")

con.execute("CREATE TEMP TABLE subacct AS SELECT * FROM subacct_base ORDER BY acctno, subacct")

# Merge with TABLE
con.execute("""
    CREATE TEMP TABLE subacct_tbl AS
    SELECT s.*, t.tfnum, t.tfid as tfid_tbl, t.tfindr02, t.tfindr03, t.tfindr05
    FROM subacct s
    LEFT JOIN table_tbl t ON s.acctno = t.acctno AND s.subacct = t.subacct
""")

# Filter for non-facility subaccounts
con.execute("""
    CREATE TEMP TABLE subtran AS
    SELECT DISTINCT * FROM subacct_tbl
    WHERE SUBSTRING(subacct, 1, 1) NOT IN ('F', 'O')
    ORDER BY acctno, transref_final
""")

# Merge with CREDPROV
con.execute("""
    CREATE TEMP TABLE suba AS
    SELECT DISTINCT s.*, c.prinamt, c.intamt, c.iisamt, c.totiisr, 
           c.writoff, c.outstand, c.avgdbal, c.nplind, c.maturedx
    FROM subtran s
    LEFT JOIN credprov c ON s.acctno = c.acctno AND s.transref_final = c.transref
    ORDER BY acctno, transref_final
""")

# ============================================================================
# Create DIRCTIND format mapping
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE dirct AS
    SELECT DISTINCT liabcode, dirctind
    FROM read_parquet('{isuba_path}')
""")

# ============================================================================
# Merge SUBA with EXTCOMM2 to create IBTDTL
# ============================================================================

con.execute("""
    CREATE TEMP TABLE ibtdtl_base AS
    SELECT 
        COALESCE(s.acctno, e.acctno) as acctno,
        COALESCE(e.subacct1, s.subacct) as subacct,
        COALESCE(s.transref_final, e.transref) as transref,
        s.*,
        e.*,
        CASE WHEN s.maturedx NOT IN (0, NULL)
             THEN TRY_STRPTIME(CAST(s.maturedx AS VARCHAR), '%y%m%d')
             ELSE NULL END as matdate,
        COALESCE(s.dirctind, d.dirctind) as dirctind_final
    FROM suba s
    FULL OUTER JOIN extcomm2_dated e ON s.acctno = e.acctno 
                                     AND s.transref_final = e.transref
    LEFT JOIN dirct d ON COALESCE(s.liabcode, e.liabcode) = d.liabcode
""")

# ============================================================================
# Merge with HSTR (history data)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE hstr AS
    SELECT DISTINCT transref, acctno, subacct, property, share, openend
    FROM read_parquet('{ihstr_path}')
    ORDER BY acctno, transref
""")

con.execute("""
    CREATE TEMP TABLE ibtdtl_hstr AS
    SELECT COALESCE(h.acctno, i.acctno) as acctno,
           COALESCE(h.subacct, i.subacct) as subacct,
           COALESCE(h.transref, i.transref) as transref,
           i.*, h.property, h.share, h.openend
    FROM ibtdtl_base i
    LEFT JOIN hstr h ON i.acctno = h.acctno AND i.transref = h.transref
    ORDER BY acctno, subacct
""")

# ============================================================================
# Add APPRLIM2, BTREL, INTRATE from ISUBA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE subb AS
    SELECT DISTINCT acctno, subacct, apprlim2, btrel, intrate
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY acctno, subacct 
                                 ORDER BY intrate DESC) as rn
        FROM read_parquet('{isuba_path}')
        WHERE apprlim2 > 0
    ) WHERE rn = 1
""")

# Merge with AVDB
con.execute(f"""
    CREATE TEMP TABLE subb_avdb AS
    SELECT s.*, a.spread, a.avgdbal as avgdbal_avdb
    FROM subb s
    LEFT JOIN read_parquet('{iavdb_path}') a ON s.acctno = a.acctno 
                                             AND s.subacct = a.subacct
""")

con.execute("""
    CREATE TEMP TABLE ibtdtl_rates AS
    SELECT i.*,
           COALESCE(s.apprlim2, i.apprlimt) as apprlim2_final,
           COALESCE(s.intrate, 0) as intrate_final,
           COALESCE(s.spread, 0) as spread_final,
           s.btrel as btrel_subb
    FROM ibtdtl_hstr i
    LEFT JOIN subb_avdb s ON i.acctno = s.acctno AND i.subacct = s.subacct
""")

# ============================================================================
# Final merge with EXTHSTR for agents
# ============================================================================

con.execute("""
    CREATE TEMP TABLE exthstr_sub AS
    SELECT DISTINCT acctno, subacct, agent1, agent2
    FROM exthstr
    WHERE agent1 != ' ' OR agent2 != ' '
""")

con.execute("""
    CREATE TABLE ibtdtl_final AS
    SELECT i.*,
           COALESCE(e.agent1, i.agent1) as agent1_final,
           COALESCE(e.agent2, i.agent2) as agent2_final,
           i.apprlim2_final as apprlimt_final,
           CASE WHEN i.creatds NOT IN (0, NULL)
                THEN TRY_STRPTIME(LPAD(CAST(i.creatds AS VARCHAR), 6, '0'), '%y%m%d')
                ELSE NULL END as issdte_final,
           CASE WHEN i.expirds NOT IN (0, NULL)
                THEN TRY_STRPTIME(LPAD(CAST(i.expirds AS VARCHAR), 6, '0'), '%y%m%d')
                ELSE NULL END as exprdate_final
    FROM ibtdtl_rates i
    LEFT JOIN exthstr_sub e ON i.acctno = e.acctno AND i.subacct = e.subacct
""")

# Save IBTDTL output
con.execute(f"COPY ibtdtl_final TO '{output_ibtdtl}'")

# ============================================================================
# Generate Summary Report (PROC TABULATE equivalent)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE summary AS
    SELECT branch, glmnemo,
           SUM(gltrntot) as gltrntot
    FROM extcomm2
    GROUP BY branch, glmnemo
""")

# Create pivot table for report
report_df = con.execute("""
    SELECT glmnemo,
           SUM(CASE WHEN branch IS NOT NULL THEN gltrntot ELSE 0 END) as total_all_branches,
           SUM(gltrntot) as total
    FROM summary
    GROUP BY glmnemo
    ORDER BY glmnemo
""").df()

# Write report
with open(output_report, 'w') as f:
    f.write(f"BANK TRADE DETAIL AS AT {rdate}\n")
    f.write("=" * 80 + "\n\n")
    f.write(f"{'GLMNEMO':<15} {'TOTAL':>20}\n")
    f.write("-" * 80 + "\n")
    
    for _, row in report_df.iterrows():
        f.write(f"{row['glmnemo']:<15} {row['total']:>20,.2f}\n")
    
    f.write("-" * 80 + "\n")
    f.write(f"{'TOTAL':<15} {report_df['total'].sum():>20,.2f}\n")

con.close()

print(f"\nProcessing complete. Output files:")
print(f"- {output_iextcom}")
print(f"- {output_ibtdtl}")
print(f"- {output_report}")
