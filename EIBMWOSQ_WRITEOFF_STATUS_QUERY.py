import duckdb
import polars as pl
from pathlib import Path

# Read REPTDATE and create variables
reptdate_df = duckdb.connect().execute("SELECT * FROM read_parquet('BNM/REPTDATE.parquet')").pl()
reptdate = reptdate_df["REPTDATE"][0]

day = reptdate.day
month = reptdate.month
year = reptdate.year % 100

if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

nowk, nowk1 = wk, wk1
reptday, reptmon, reperyear = f"{day:02d}", f"{month:02d}", f"{year:02d}"

# Define column selections
lnwo_cols = ["ACCTNO", "NOTENO", "BRANCH", "PRODUCT", "ORIPRODUCT", "SPWO", "ISWO", "WOAMT", "WDB", "ACSTATUS", "WODATE", "WOMM"]
odwo_cols = ["ACCTNO", "BRANCH", "PRODUCT", "ORIPRODUCT", "SPWO", "ISWO", "WOAMT", "WDB", "ACSTATUS", "WODATE", "WOMM"]

# Process LNFILE with fixed-width parsing
lnfile_schema = {
    "ACCTNO": (0, 10), "NOTENO": (10, 5), "BRANCH": (15, 7), "COSTCTR": (22, 7),
    "ORIPRODUCT": (29, 6), "NOTE_INT": (35, 1), "PRODUCT": (36, 3), "NTYPE": (39, 3),
    "CURBAL": (42, 15), "ACCRUAL": (57, 15), "OUT_BAL": (72, 15), "SPWO": (87, 15),
    "ISWO": (102, 15), "TOT_PAY_AMT": (117, 15), "TOT_DBT_AMT": (132, 15), 
    "ORI_WDB": (147, 15), "NISWO": (162, 15), "NSPWO": (177, 15), "WDB": (192, 15),
    "WO_IND": (207, 1), "TOTAL_IISR": (208, 15), "WOYR": (223, 4), 
    "WOMM": (227, 2), "WODD": (229, 2)
}

lnfile_df = pl.read_csv("LNFILE", has_header=False, new_columns=["raw"])
for col, (start, length) in lnfile_schema.items():
    lnfile_df = lnfile_df.with_columns(
        pl.col("raw").str.slice(start, length).str.strip_chars().alias(col)
    )

lnfile_df = lnfile_df.drop("raw").with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("NOTENO").cast(pl.Int64),
    pl.col("BRANCH").cast(pl.Int64),
    pl.col("COSTCTR").cast(pl.Int64),
    pl.col("ORIPRODUCT").cast(pl.Float64),
    pl.col("PRODUCT").cast(pl.Int64),
    pl.col("NTYPE").cast(pl.Int64),
    pl.col("CURBAL").cast(pl.Float64),
    pl.col("ACCRUAL").cast(pl.Float64),
    pl.col("OUT_BAL").cast(pl.Float64),
    pl.col("SPWO").cast(pl.Float64),
    pl.col("ISWO").cast(pl.Float64),
    pl.col("TOT_PAY_AMT").cast(pl.Float64),
    pl.col("TOT_DBT_AMT").cast(pl.Float64),
    pl.col("ORI_WDB").cast(pl.Float64),
    pl.col("NISWO").cast(pl.Float64),
    pl.col("NSPWO").cast(pl.Float64),
    pl.col("WDB").cast(pl.Float64),
    pl.col("TOTAL_IISR").cast(pl.Float64),
    pl.col("WOYR").cast(pl.Int64),
    pl.col("WOMM").cast(pl.Int64),
    pl.col("WODD").cast(pl.Int64)
]).with_columns([
    (pl.col("NISWO") + pl.col("NSPWO")).alias("WOAMT"),
    pl.when(pl.col("WDB") <= 2.00).then(pl.lit("W")).otherwise(pl.lit("P")).alias("ACSTATUS"),
    pl.date(pl.col("WOYR"), pl.col("WOMM"), pl.col("WODD")).alias("WODATE")
])

lnsq_df = lnfile_df.filter(~pl.col("COSTCTR").is_between(3000, 4999)).select(lnwo_cols)
ilnsq_df = lnfile_df.filter(pl.col("COSTCTR").is_between(3000, 4999)).select(lnwo_cols)

# Process ODFILE with fixed-width parsing
odfile_schema = {
    "STATUS": (0, 1), "ACCTNO": (4, 11), "SPWO": (15, 11), "ISWO": (26, 11),
    "WDB": (37, 11), "IISR": (48, 11), "RC": (59, 11), "ORIPRODUCT": (106, 3),
    "BRANCH": (109, 3), "COSTCTR": (112, 7), "WOTYPE": (120, 1), "ACSTATUS": (121, 1),
    "LEDGBAL": (122, 13), "PRODUCT": (169, 3), "WOYR": (172, 4), "WOMM": (176, 2),
    "WODD": (178, 2)
}

odfile_df = pl.read_csv("ODFILE", has_header=False, new_columns=["raw"])
for col, (start, length) in odfile_schema.items():
    odfile_df = odfile_df.with_columns(
        pl.col("raw").str.slice(start, length).str.strip_chars().alias(col)
    )

odfile_df = odfile_df.drop("raw").with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("SPWO").cast(pl.Float64),
    pl.col("ISWO").cast(pl.Float64),
    pl.col("WDB").cast(pl.Float64),
    pl.col("IISR").cast(pl.Float64),
    pl.col("RC").cast(pl.Float64),
    pl.col("ORIPRODUCT").cast(pl.Int64),
    pl.col("BRANCH").cast(pl.Int64),
    pl.col("COSTCTR").cast(pl.Int64),
    pl.col("LEDGBAL").cast(pl.Float64),
    pl.col("PRODUCT").cast(pl.Int64),
    pl.col("WOYR").cast(pl.Int64),
    pl.col("WOMM").cast(pl.Int64),
    pl.col("WODD").cast(pl.Int64)
]).with_columns([
    (pl.col("ISWO") + pl.col("SPWO")).alias("WOAMT"),
    pl.date(pl.col("WOYR"), pl.col("WOMM"), pl.col("WODD")).alias("WODATE")
]).filter(pl.col("STATUS") != "S").drop("STATUS")

odsq_df = odfile_df.filter(~pl.col("COSTCTR").is_between(3000, 4999)).select(odwo_cols)
iodsq_df = odfile_df.filter(pl.col("COSTCTR").is_between(3000, 4999)).select(odwo_cols)

# Create final datasets
wosq_final = pl.concat([lnsq_df, odsq_df]).filter(pl.col("WOMM") == int(reptmon)).drop("WOMM").sort(["ACCTNO", "NOTENO"])
iwosq_final = pl.concat([ilnsq_df, iodsq_df]).filter(pl.col("WOMM") == int(reptmon)).drop("WOMM").sort(["ACCTNO", "NOTENO"])

# Write output files
Path("WOSQ").mkdir(exist_ok=True)
Path("IWOSQ").mkdir(exist_ok=True)

wosq_final.write_parquet(f"WOSQ/WO{reptmon}{nowk}{reperyear}.parquet")
iwosq_final.write_parquet(f"IWOSQ/IWO{reptmon}{nowk}{reperyear}.parquet")
wosq_final.write_csv(f"WOSQ/WO{reptmon}{nowk}{reperyear}.csv")
iwosq_final.write_csv(f"IWOSQ/IWO{reptmon}{nowk}{reperyear}.csv")
