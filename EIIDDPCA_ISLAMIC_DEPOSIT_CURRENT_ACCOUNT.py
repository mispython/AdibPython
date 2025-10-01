# ============================================================
# Job Name : EIIDDPCA_DEPOSIT_ACCOUNT_ICA
# Purpose  : Conversion of SAS EIIDDPCA program (Deposit Extraction)
# Engine   : DuckDB + PyArrow (Polars only if needed)
# ============================================================

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
import os

# ------------------------------------------------------------------
# 1. Load base datasets (replace with actual paths)
# ------------------------------------------------------------------
cis_deposit = ds.dataset("CIS/DEPOSIT.parquet", format="parquet").to_table()
dptrbl_reptdate = ds.dataset("DPTRBL/REPTDATE.parquet", format="parquet").to_table()
dptrbl_current = ds.dataset("DPTRBL/CURRENT.parquet", format="parquet").to_table()
od_overdft = ds.dataset("OD/OVERDFT.parquet", format="parquet").to_table()
signa_smsacc = ds.dataset("SIGNA/SMSACC.parquet", format="parquet").to_table()
drcrca_accum = ds.dataset("DRCRCA/ACCUM.parquet", format="parquet").to_table()

# ------------------------------------------------------------------
# 2. Build CIS subset
# ------------------------------------------------------------------
cis = cis_deposit.filter(
    pc.equal(cis_deposit["SECCUST"], pa.scalar("901"))
).select(["ACCTNO", "CITIZEN", "BIRTHDAT", "RACE", "CUSTNAM1"])

# Derive DOB (replicating SAS substrings)
dob_dd = pc.utf8_slice_codeunits(cis["BIRTHDAT"], 0, 2)
dob_mm = pc.utf8_slice_codeunits(cis["BIRTHDAT"], 2, 2)
dob_yy = pc.utf8_slice_codeunits(cis["BIRTHDAT"], 4, 4)

cis = cis.append_column(
    "DOBCIS", 
    pc.binary_join_element_wise(
        dob_yy, pc.binary_join_element_wise(dob_mm, dob_dd, ""), "-"
    )
)

cis = cis.rename_columns(["ACCTNO", "COUNTRY", "BIRTHDAT", "RACE", "NAME", "DOBCIS"])

# ------------------------------------------------------------------
# 3. Register in DuckDB
# ------------------------------------------------------------------
con = duckdb.connect(database=":memory:")

con.register("cis", cis)
con.register("reptdate", dptrbl_reptdate)
con.register("current", dptrbl_current)
con.register("overdft", od_overdft)
con.register("saacc", signa_smsacc)
con.register("accum", drcrca_accum)

# ------------------------------------------------------------------
# 4. REPTDATE transformation (example week classification)
# ------------------------------------------------------------------
reptdate_transformed = con.execute("""
    SELECT *,
           CASE 
               WHEN EXTRACT(DAY FROM REPTDATE) BETWEEN 1 AND 8  THEN 1
               WHEN EXTRACT(DAY FROM REPTDATE) BETWEEN 9 AND 15 THEN 2
               WHEN EXTRACT(DAY FROM REPTDATE) BETWEEN 16 AND 22 THEN 3
               ELSE 4
           END AS WK
    FROM reptdate
""").arrow()

# ------------------------------------------------------------------
# 5. Merge CURRENT + OVERDFT
# ------------------------------------------------------------------
curr = con.execute("""
    SELECT 
        c.*,
        o.CRRINI AS FAACRR,
        o.CRRNOW AS CRRCODE,
        o.ASCORE_PERM,
        o.ASCORE_LTST,
        o.ASCORE_COMM
    FROM current c
    LEFT JOIN overdft o USING(ACCTNO)
""").arrow()

con.register("curr", curr)

# ------------------------------------------------------------------
# 6. Merge CIS + SAACC
# ------------------------------------------------------------------
ica = con.execute("""
    SELECT 
        cis.ACCTNO,
        cis.NAME,
        curr.*,
        saacc.ESIGNATURE
    FROM cis
    JOIN curr ON cis.ACCTNO = curr.ACCTNO
    LEFT JOIN saacc ON cis.ACCTNO = saacc.ACCTNO
""").arrow()

# ------------------------------------------------------------------
# 7. Merge with ACCUM
# ------------------------------------------------------------------
ica_final = con.execute("""
    SELECT i.*, a.*
    FROM ica i
    LEFT JOIN accum a ON i.ACCTNO = a.ACCTNO
""").arrow()

# ------------------------------------------------------------------
# 8. Save result (Parquet + CSV)
# ------------------------------------------------------------------
os.makedirs("output", exist_ok=True)
out_parquet = "output/EIIDDPCA_DEPOSIT_ACCOUNT_ICA.parquet"
out_csv     = "output/EIIDDPCA_DEPOSIT_ACCOUNT_ICA.csv"

# Save Parquet
pq.write_table(ica_final, out_parquet)

# Save CSV
con.register("ica_final", ica_final)
con.execute(f"COPY ica_final TO '{out_csv}' (HEADER, DELIMITER ',')")

print(" Job EIIDDPCA_DEPOSIT_ACCOUNT_ICA completed.")
print(f"   - Parquet: {out_parquet}")
print(f"   - CSV    : {out_csv}")
