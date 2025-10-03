import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime

# -----------------------------
# 1. READ BASE TRANSACTION FILE
# -----------------------------
# Assume DPTRBL, CUST, CISTAXID, DPCLOS are parquet inputs

con = duckdb.connect()

# Example: read DPTRBL into DuckDB
con.execute("""
    CREATE TABLE DPTRBL AS
    SELECT * FROM read_parquet('DPTRBL.parquet')
""")

# Extract REPTDATE from TBDATE (packed decimal -> string -> date)
con.execute("""
    CREATE TABLE DEPOSIT_REPTDATE AS
    SELECT 
        TRY_CAST(SUBSTRING(LPAD(CAST(TBDATE AS BIGINT)::VARCHAR, 11, '0'),1,8) AS DATE) AS REPTDATE
    FROM DPTRBL
    LIMIT 1
""")

# -----------------------------
# 2. PBGOLD BASE FILTER
# -----------------------------
con.execute("""
    CREATE TABLE PBGOLD AS
    SELECT 
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCH,
        ACCTNO,
        NAME,
        CLOSEDT,
        REOPENDT,
        CUSTCODE,
        INTPAID,
        ODPLAN,
        RATE1, RATE2, RATE3, RATE4, RATE5,
        TODRATE, FLATRATE, BASERATE,
        ODSTAT, ORGCODE, ORGTYPE,
        LIMIT1, LIMIT2, LIMIT3, LIMIT4, LIMIT5,
        INTYTD, FEEPD, PURPOSE,
        COL1, COL2, COL3, COL4, COL5,
        SECTOR, USER2, USER3, RISKCODE,
        LEDGBAL, OPENIND, LASTTRAN,
        AVGAMT, PRODUCT, RACE, DEPTYPE,
        INT1, INTPD, INTPLAN, CURBAL, CHQFLOAT,
        INT2, MTDLOWBA, BENINTPD, STATE,
        APPRLIMT, ODINTCHR, ODINTACC,
        CURCODE, INTRATE, YTDAVAMT,
        BDATE, INACTIVE, SECOND,
        ODXSAMT, BONUTYPE, SERVICE, BONUSANO,
        USER5, EXODDATE, TEMPODDT, SCHIND,
        PREVBRNO, AVGBAL, COSTCTR, CRRCODE,
        CCRICODE, FAACRR, CENSUST,
        ACCPROF, MAXPROF, INTRSTPD,
        MTDAVBAL, OPENDT
    FROM DPTRBL
    WHERE BANKNO = 33 
      AND REPTNO = 1001
      AND FMTCODE IN (1,10,22)
      AND PRODUCT IN (480,481,482,483,484)
      AND CURCODE = 'XAU'
""")

# -----------------------------
# 3. DATE CLEANUP
# -----------------------------
# Convert numeric YYYYMMDD → DATE
def conv_date(val: int):
    if val in (0, None):
        return None
    s = str(val).zfill(8)
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except Exception:
        return None

# Use Polars for post-processing
tbl = con.execute("SELECT * FROM PBGOLD").arrow()
df = pl.from_arrow(tbl)

# Date conversions
for col in ["BDATE", "OPENDT", "REOPENDT", "CLOSEDT"]:
    df = df.with_columns(
        pl.col(col).apply(conv_date).alias(col)
    )

df = df.with_columns(
    pl.col("LASTTRAN").apply(lambda v: conv_date(v) if v else None).alias("LASTTRAN")
)

# -----------------------------
# 4. MERGE WITH CUST
# -----------------------------
cust = pl.read_parquet("CUST.parquet").filter(pl.col("SECCUST")=="901")
df = df.join(cust, on="ACCTNO", how="left")

# -----------------------------
# 5. MERGE WITH TAXID
# -----------------------------
taxid = pl.read_parquet("CISTAXID.parquet")
taxid = taxid.with_columns(
    pl.when(pl.col("NEWIC").str.strip_chars() == "")
      .then(pl.col("OLDIC"))
      .otherwise(pl.col("NEWIC"))
      .alias("ICNO")
)
taxid = taxid.select(["CUSTNO","ICNO"])
df = df.join(taxid, on="CUSTNO", how="left")

# -----------------------------
# 6. KEEP ONLY REQUIRED FIELDS
# -----------------------------
gold = df.select([
    "BRANCH","ACCTNO","PRODUCT","PURPOSE","OPENDT","LEDGBAL",
    "OPENIND","LASTTRAN","AVGAMT","RACE","DEPTYPE","STATE",
    "CURCODE","USER3","BDATE","CURBAL","CUSTCODE","CLOSEDT",
    "RATE","COSTCTR","CITIZEN","BONUSANO","SECOND","REOPENDT"
])

# -----------------------------
# 7. MERGE WITH DPCLOS (LATEST CLOSEDT)
# -----------------------------
dpclos = pl.read_parquet("DPCLOS.parquet")
dpclos = dpclos.sort(["ACCTNO","CLOSEDT"], descending=[False,True])
dpclos = dpclos.unique(subset="ACCTNO", keep="first")

gold = gold.join(
    dpclos.rename({"CLOSEDT":"CLOSEDTX","RCODE":"RCODEX"}),
    on="ACCTNO", how="left"
)

gold = gold.with_columns([
    pl.when(pl.col("OPENIND").is_in(["B","C","P"]))
      .then(pl.col("CLOSEDTX")).otherwise(pl.col("CLOSEDT")).alias("CLOSEDT"),
    pl.when(pl.col("OPENIND").is_in(["B","C","P"]))
      .then(pl.col("RCODEX")).alias("RCODE")
]).drop(["CLOSEDTX","RCODEX"])

# -----------------------------
# 8. OUTPUT FINAL
# -----------------------------
out_name = f"DEPOSIT_GOLD{datetime.now().strftime('%y%m%d')}"
pq.write_table(gold.to_arrow(), f"{out_name}.parquet")
csv.write_csv(gold.to_arrow(), f"{out_name}.csv")

print(f" Output written: {out_name}.parquet and {out_name}.csv")
