# EIIDLIME_ISLAMIC_LIMIT_EXTRACTOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime

# Configuration
base_path = Path(".")
dplimit_path = base_path / "DPLIMIT"
depos_path = base_path / "DEPOS"
dpcbr_path = base_path / "DPCBR"
limit_path = base_path / "LIMIT"
limit_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIIDLIME - Islamic Limit Data Extractor")

# Step 1: Process REPTDATE
reptdate_query = f"SELECT * FROM read_parquet('{dplimit_path / \"REPTDATE.parquet\"}')"
reptdate_df = conn.execute(reptdate_query).arrow()

if len(reptdate_df) > 0:
    reptdate = reptdate_df['REPTDATE'][0].as_py()
    rept_day = reptdate.day
    
    if rept_day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif rept_day == 15:
        sdd, wk, wk1 = 9, '2', '1'  
    elif rept_day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
    
    mm = reptdate.month
    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm
        
    sdate = datetime(reptdate.year, mm, sdd)
    
    nowk, nowk1 = wk, wk1
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reperyear = reptdate.strftime('%y')
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime('%y%m%d')
    sdate_str = sdate.strftime('%d%m%y')
    rept_dt = reptdate.strftime('%Y%m%d')

# Save REPTDATE
pq.write_table(reptdate_df.select(['REPTDATE']), limit_path / "REPTDATE.parquet")

# Step 2: Process OVERDFT and CURRENT data
odlm_query = f"SELECT * FROM read_parquet('{dplimit_path / \"OVERDFT.parquet\"}') ORDER BY ACCTNO"
odlm_df = conn.execute(odlm_query).arrow()

curr_query = f"SELECT ACCTNO, CURBAL, OMNILOAN, OMNITRDB FROM read_parquet('{depos_path / \"CURRENT.parquet\"}') ORDER BY ACCTNO"
curr_df = conn.execute(curr_query).arrow()

# Merge ODLM with CURRENT
odlm_merged_query = """
SELECT 
    o.*,
    COALESCE(c.CURBAL, 0) as CURBAL,
    c.OMNILOAN,
    c.OMNITRDB,
    CASE WHEN o.LMTRATE > 0 THEN (o.INTINCGR * o.LMTADJF) / o.LMTRATE ELSE 0 END as INTINCNT
FROM odlm_df o
LEFT JOIN curr_df c ON o.ACCTNO = c.ACCTNO
WHERE o.ACCTNO IS NOT NULL
"""
odlm_merged = conn.execute(odlm_merged_query).arrow()

# Step 3: Process DPCBR data
dpcbr_file = dpcbr_path / "CORDATA.txt"
if dpcbr_file.exists():
    odf_query = f"""
    SELECT 
        CAST(SUBSTR(line, 1, 6) as INTEGER) as ACCTNO,
        CAST(SUBSTR(line, 35, 6) as DOUBLE) as INTINCME,
        CAST(SUBSTR(line, 41, 6) as DOUBLE) as COMMTFEE
    FROM read_csv('{dpcbr_file}', header=false, columns={{'line': 'VARCHAR'}})
    WHERE LENGTH(line) >= 46
    """
    odf_df = conn.execute(odf_query).arrow()
else:
    odf_df = pa.table({
        'ACCTNO': pa.array([], pa.int32()),
        'INTINCME': pa.array([], pa.float64()),
        'COMMTFEE': pa.array([], pa.float64())
    })

# Step 4: Create final Islamic limit datasets
limit_columns = [
    'ACCTNO', 'BRANCH', 'LMTID', 'LMTDESC', 'LMTINDEX', 'LMTBASER',
    'LMTADJF', 'LMTRATE', 'LMTTYPE', 'LMTAMT', 'LMTCOLL',
    'PRODUCT', 'OPENIND', 'STATE', 'RISKCODE', 'EXCESSDT', 'TODDATE',
    'ODSTATUS', 'APPRLIMT', 'REVIEWDT', 'LMTSTART', 'LMTENDDT',
    'LMTTERM', 'LMTTRMID', 'LMTINCR', 'ODTEMADJ', 'ODFEECTD',
    'COMMTFEE', 'INTINCME', 'CURBAL', 'ODFDIND', 'FDACNO', 'FDRCNO',
    'COMMFERT', 'LINEFEWV', 'LFEEYTD', 'INTBLYTD', 'INTINCGR',
    'ODDATE', 'ODLMTDTE', 'INTINCNT', 'AVRCRBAL', 'AUTOPRIC',
    'REFIN_FLG', 'OLD_FI_CD', 'OLD_MACC_NO', 'OLD_SUBACC_NO',
    'CLIMATE_PRIN_TAXONOMY_CLASS', 'CLF_WAKLH_FLG',
    'CRISPURP', 'OMNILOAN', 'OMNITRDB', 'CLF_EXP_DT'
]

final_query = f"""
WITH merged_data AS (
    SELECT 
        COALESCE(o.ACCTNO, d.ACCTNO) as ACCTNO,
        COALESCE(d.INTINCME, 0.00) as INTINCME,
        COALESCE(d.COMMTFEE, 0.00) as COMMTFEE,
        o.* EXCLUDE (ACCTNO, INTINCME, COMMTFEE)
    FROM odlm_merged o
    LEFT JOIN odf_df d ON o.ACCTNO = d.ACCTNO
    WHERE o.ACCTNO IS NOT NULL
),
processed_data AS (
    SELECT *,
        CASE WHEN LMTAMT = 0 THEN '' ELSE AUTOPRIC END as AUTOPRIC_ADJ,
        CASE WHEN EXCESSDT > 0 THEN CAST(SUBSTR(CAST(EXCESSDT as VARCHAR), 1, 8) as DATE) ELSE NULL END as EXCESSDT_CONV,
        CASE WHEN TODDATE > 0 THEN CAST(SUBSTR(CAST(TODDATE as VARCHAR), 1, 8) as DATE) ELSE NULL END as TODDATE_CONV,
        CASE WHEN REVIEWDT > 0 THEN CAST(SUBSTR(CAST(REVIEWDT as VARCHAR), 1, 8) as DATE) ELSE NULL END as REVIEWDT_CONV,
        CASE WHEN LMTSTART > 0 THEN CAST(SUBSTR(CAST(LMTSTART as VARCHAR), 1, 8) as DATE) ELSE NULL END as LMTSTART_CONV,
        CASE WHEN LMTENDDT > 0 THEN CAST(SUBSTR(CAST(LMTENDDT as VARCHAR), 1, 8) as DATE) ELSE NULL END as LMTENDDT_CONV,
        CASE WHEN ODLMTDTE IS NOT NULL THEN 
            CAST(SUBSTR(CAST(ODLMTDTE as VARCHAR), 5, 4) || '-' ||
                 SUBSTR(CAST(ODLMTDTE as VARCHAR), 3, 2) || '-' ||
                 SUBSTR(CAST(ODLMTDTE as VARCHAR), 1, 2) as DATE)
            ELSE NULL END as ODDATE_CONV
    FROM merged_data
    WHERE BRANCH != 0
)
SELECT 
    {', '.join([f'{col}' if col != 'AUTOPRIC' else 'AUTOPRIC_ADJ as AUTOPRIC' 
               for col in limit_columns if col not in ['EXCESSDT', 'TODDATE', 'REVIEWDT', 'LMTSTART', 'LMTENDDT', 'ODDATE']])},
    EXCESSDT_CONV as EXCESSDT,
    TODDATE_CONV as TODDATE,
    REVIEWDT_CONV as REVIEWDT,
    LMTSTART_CONV as LMTSTART,
    LMTENDDT_CONV as LMTENDDT,
    ODDATE_CONV as ODDATE
FROM processed_data
"""
final_df = conn.execute(final_query).arrow()

# Save outputs
pq.write_table(final_df, limit_path / f"ILM_{rept_dt}.parquet")
csv.write_csv(final_df, limit_path / f"ILM_{rept_dt}.csv")
pq.write_table(final_df, limit_path / f"ILM{rdate}.parquet")
csv.write_csv(final_df, limit_path / f"ILM{rdate}.csv")

print(f"Completed: {len(final_df)} Islamic limit records processed")
conn.close()
