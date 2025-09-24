import polars as pl
import pyarrow as pa
import duckdb
from datetime import date, timedelta

# 1. REPTDATE setup
reptdate = date.today() - timedelta(days=1)
day = reptdate.day
month = reptdate.month
year = reptdate.year % 100  # 2-digit year

if day == 8:
    wk, wk1, sdd = "1", "4", 1
elif day == 15:
    wk, wk1, sdd = "2", "1", 9
elif day == 22:
    wk, wk1, sdd = "3", "2", 16
else:
    wk, wk1, sdd = "4", "3", 23

mm1 = (month - 1) if wk == "1" else month
if mm1 == 0:
    mm1 = 12

mm2 = month if wk == "4" else (month - 1 if month > 1 else 12)

# symbolic vars
NOWK, NOWK1 = wk, wk1
REPTMON, REPTMON1, REPTMON2 = f"{month:02}", f"{mm1:02}", f"{mm2:02}"
REPTYEAR = f"{year:02}"
RDATE = reptdate.strftime("%d/%m/%y")
REPTDAY = f"{day:02}"
MDATE = reptdate.strftime("%Y%m%d")

# 2. Load SUBACRED fixed-width
subacred = pl.read_csv(
    "SUBACRED.txt",
    has_header=False,
    new_columns=["raw"],
    infer_schema_length=0,
)

subacred = subacred.with_columns([
    subacred["raw"].str.slice(6, 3).alias("BRANCH"),
    subacred["raw"].str.slice(12, 10).alias("ACCTNO"),
    subacred["raw"].str.slice(42, 10).alias("NOTENO"),
    subacred["raw"].str.slice(216, 13).alias("AANO"),
]).select(["BRANCH", "ACCTNO", "NOTENO", "AANO"])

# 3. Load ELNA7 (assume CSV/Parquet export)
elna7 = pl.read_parquet("ELNA7.parquet")

# renames like SAS RENAME
elna7 = elna7.rename({
    "AANOSEC7": "AANO",
    "PROPERT": "CPRPROPD",
    "LNAREAUN": "LANDUNIT",
    "BUILTUP": "BLTNAREA",
    "BLTUPUN": "BLTNUNIT",
    "FREELEAS": "HOLDEXPD",
    "USEDSA": "OWNOCUPY",
    "CMVALUE": "CDOLARV",
    "FSVALUE": "CPRFORSV",
    "VALUERI": "CPRVALIN",
    "VALUENM": "CPRVALU1",
    "ADDRESS6": "CPOSCODE",
    "VIND2": "CPRVALIN2",
    "VNAME": "CPRVALU2",
    "PROJNAME": "PRJCTNAM",
    "LNUSDCAT": "CPRLANDU",
})

# address concatenation
elna7 = elna7.with_columns([
    (pl.concat_str([pl.col("ADDRESS1"), pl.col("ADDRESS2")], separator=",")
     .fill_null(pl.col("ADDRESS2")))
     .alias("CPRPARC1"),
    pl.col("ADDRESS3").alias("CPRPARC2"),
    pl.col("ADDRESS4").alias("CPRPARC3"),
    pl.col("ADDRESS5").alias("CPRPARC4"),
])

elna7 = elna7.with_columns([
    (pl.concat_str([
        pl.col("CPRPARC1"), pl.col("CPRPARC2"),
        pl.col("CPRPARC3"), pl.col("CPRPARC4")
    ], separator=",")).alias("ADDRESS"),
    pl.lit(1).alias("CPRRANKC"),
    pl.col("VALUEDT").dt.strftime("%d/%m/%Y").alias("CPRVALDT")
])

# 4. Merge SUBACRED + ELNA7 on AANO
combined = subacred.join(elna7, on="AANO", how="inner")

# 5. Write CCMS dataset (Parquet for CMS system)
combined.write_parquet(f"CMS_CCMS_{REPTMON}{REPTYEAR}.parquet")

# 6. Reformat numeric/dates with DuckDB SQL
con = duckdb.connect()
con.register("combined", combined.to_arrow())

final = con.execute("""
    SELECT 
        BRANCH::INT,
        ACCTNO,
        AANO,
        CAST(CCOLLNO AS BIGINT) AS CCOLLNO,
        TRY_CAST(EXPDATE AS DATE) AS EXPDATE,
        TRY_CAST(FIREDATE AS DATE) AS FIREDATE,
        TRY_CAST(CPRVALDT AS DATE) AS CPRVALDT,
        *
    FROM combined
""").pl()

final.write_parquet(f"CMS_CCMS_{REPTMON}{REPTYEAR}_final.parquet")
