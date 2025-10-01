import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import datetime

# Connect DuckDB in-memory
con = duckdb.connect()

# ------------------------------------------------------------------
# Load base tables (all parquet input)
# ------------------------------------------------------------------
con.execute("CREATE OR REPLACE TABLE dptrbl_reptdate AS SELECT * FROM 'DPTRBL_REPTDATE.parquet'")
con.execute("CREATE OR REPLACE TABLE cis_deposit AS SELECT * FROM 'CIS_DEPOSIT.parquet'")
con.execute("CREATE OR REPLACE TABLE dptrbl_saving AS SELECT * FROM 'DPTRBL_SAVING.parquet'")
con.execute("CREATE OR REPLACE TABLE dptrbl_uma AS SELECT * FROM 'DPTRBL_UMA.parquet'")
con.execute("CREATE OR REPLACE TABLE signa_smsacc AS SELECT * FROM 'SIGNA_SMSACC.parquet'")
con.execute("CREATE OR REPLACE TABLE mismtd_savg AS SELECT * FROM 'MISMTD_SAVG.parquet'")

# ------------------------------------------------------------------
# Step 1: REPTDATE logic (derive WK, MM, year, etc.)
# ------------------------------------------------------------------
reptdate_tbl = con.execute("""
    SELECT *,
           CASE 
               WHEN day(REPTDATE) BETWEEN 1 AND 8 THEN '1'
               WHEN day(REPTDATE) BETWEEN 9 AND 15 THEN '2'
               WHEN day(REPTDATE) BETWEEN 16 AND 22 THEN '3'
               ELSE '4'
           END AS WK,
           CASE 
               WHEN (CASE 
                       WHEN day(REPTDATE) BETWEEN 1 AND 8 THEN '1'
                       WHEN day(REPTDATE) BETWEEN 9 AND 15 THEN '2'
                       WHEN day(REPTDATE) BETWEEN 16 AND 22 THEN '3'
                       ELSE '4' END) <> '4'
                    THEN month(REPTDATE)-1
               ELSE month(REPTDATE)
           END AS MM,
           year(REPTDATE) AS REPTYEAR,
           month(REPTDATE) AS REPTMON,
           day(REPTDATE) AS REPTDAY
    FROM dptrbl_reptdate
""").arrow()

row0 = reptdate_tbl.to_pydict()
NOWK     = row0["WK"][0]
REPTMON  = str(row0["REPTMON"][0]).zfill(2)
REPTMON1 = str(row0["MM"][0] if row0["MM"][0] != 0 else 12).zfill(2)
REPTYEAR = str(row0["REPTYEAR"][0])
REPTDAY  = str(row0["REPTDAY"][0]).zfill(2)

# ------------------------------------------------------------------
# Step 2: CIS dataset
# ------------------------------------------------------------------
cis_tbl = con.execute("""
    SELECT 
        ACCTNO,
        CITIZEN AS COUNTRY,
        custnam1 AS NAME,
        substr(BIRTHDAT,1,2)::INT AS BDD,
        substr(BIRTHDAT,3,2)::INT AS BMM,
        substr(BIRTHDAT,5,4)::INT AS BYY,
        RACE
    FROM cis_deposit
    WHERE SECCUST = '901'
""").arrow()

dobcis = [
    datetime.date(r["BYY"], r["BMM"], r["BDD"]) 
    if r["BDD"] and r["BMM"] and r["BYY"] else None
    for r in cis_tbl.to_pylist()
]
cis_tbl = cis_tbl.append_column("DOBCIS", pa.array(dobcis))

con.register("cis_tbl", cis_tbl)
cis_tbl = con.execute("""
    SELECT * FROM cis_tbl
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO)=1
""").arrow()

# ------------------------------------------------------------------
# Step 3: Monthly logic for SA / UMA
# ------------------------------------------------------------------
if NOWK != "4" and REPTMON == "01":
    con.execute("CREATE OR REPLACE TABLE sa AS SELECT *, 0 AS FEEYTD FROM dptrbl_saving")
    con.execute("CREATE OR REPLACE TABLE uma AS SELECT *, 0 AS FEEYTD FROM dptrbl_uma")
else:
    con.execute(f"CREATE OR REPLACE TABLE crmsa AS SELECT * FROM 'MNICRM_SA{REPTMON1}.parquet'")
    con.execute(f"CREATE OR REPLACE TABLE crmsuma AS SELECT * FROM 'MNICRM_UMA{REPTMON1}.parquet'")
    con.execute("CREATE OR REPLACE TABLE sa AS SELECT a.* FROM dptrbl_saving a LEFT JOIN crmsa b USING(ACCTNO)")
    con.execute("CREATE OR REPLACE TABLE uma AS SELECT a.* FROM dptrbl_uma a LEFT JOIN crmsuma b USING(ACCTNO)")

# ------------------------------------------------------------------
# Step 4: Merge SA + UMA into SAVG
# ------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE savg AS
    SELECT * FROM sa
    UNION ALL
    SELECT * FROM uma
""")

# ------------------------------------------------------------------
# Step 5: Apply data transformations
# ------------------------------------------------------------------
savg_tbl = con.execute("""
    SELECT * ,
           substr(STATCD,4,1) AS ACSTATUS4,
           substr(STATCD,5,1) AS ACSTATUS5,
           substr(STATCD,6,1) AS ACSTATUS6,
           substr(STATCD,7,1) AS ACSTATUS7,
           substr(STATCD,8,1) AS ACSTATUS8,
           substr(STATCD,9,1) AS ACSTATUS9,
           substr(STATCD,10,1) AS ACSTATUS10,
           CASE WHEN PURPOSE='4' THEN '1' ELSE PURPOSE END AS PURPOSE_CLEAN
    FROM savg
    WHERE OPENIND NOT IN ('B','C','P')
""").arrow()
con.register("savg_tbl", savg_tbl)

# ------------------------------------------------------------------
# Step 6: Final MERGE (CIS + SAVG + SMSACC + MISMTD)
# ------------------------------------------------------------------
final_tbl = con.execute(f"""
    SELECT 
        a.ACCTNO,
        a.COUNTRY,
        a.DOBCIS,
        a.RACE,
        b.*,
        c.*,
        d.*
    FROM cis_tbl a
    JOIN savg_tbl b USING(ACCTNO)
    LEFT JOIN signa_smsacc c USING(ACCTNO)
    LEFT JOIN mismtd_savg d USING(ACCTNO)
""").arrow()

# ------------------------------------------------------------------
# Save outputs (Parquet + CSV)
# ------------------------------------------------------------------
output_parquet = f"saving_isa_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
output_csv     = f"saving_isa_{REPTYEAR}{REPTMON}{REPTDAY}.csv"

pq.write_table(final_tbl, output_parquet)
con.register("final_tbl", final_tbl)
con.execute(f"COPY final_tbl TO '{output_csv}' (HEADER, DELIMITER ',')")

print(f"EIBDDPSA job completed. Output saved as {output_parquet} and {output_csv}")
