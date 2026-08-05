from __future__ import annotations
from pathlib import Path
from datetime import date
import polars as pl
import pyarrow.parquet as pq

# -------- paths --------
BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL")
DEPOSIT_REPTDATE = BASE / "SAP.PBB.MNITB" / "REPTDATE.parquet"         # DEPOSIT.REPTDATE
GLFILE_PATH      = BASE / "glfile.txt"           # GLFILE
STORE_OUT        = BASE                      # STORE lib
output = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMNLGL/"
STORE_OUT.mkdir(parents=True, exist_ok=True)

# -------- helpers --------
def ddmmyy8_to_date(s: str) -> date:
    dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
    yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
    return date(yy, mm, dd)

def round_thousands(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.with_columns([((pl.col(c) / 1000).round(0) / 1000).alias(c) for c in cols])

def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path)

def split_and_write(R: pl.DataFrame, stub: str, y: str, m: str, d: str):
    T  = R.with_columns([
        pl.col("ITEM").str.slice(0,1).alias("S1"),
        pl.col("ITEM").str.slice(1,1).alias("S2"),
    ])
    A  = T.filter(pl.col("S1") == "A").drop("S1")
    NA = T.filter(pl.col("S1") != "A").drop("S1")

    GLRMP   = A.filter(pl.col("S2") == "1").drop("S2")
    GLUTRMP = A.filter(pl.col("S2") == "2").drop("S2")
    GLFXP   = NA.filter(pl.col("S2") == "1").drop("S2")
    GLUTFXP = NA.filter(pl.col("S2") == "2").drop("S2")

    def fn(tag: str) -> Path: return STORE_OUT / f"{tag}{stub}{y}{m}{d}.parquet"
    write_parquet(GLRMP,   fn("GLRMP"))
    write_parquet(GLFXP,   fn("GLFXP"))
    write_parquet(GLUTRMP, fn("GLUTRMP"))
    write_parquet(GLUTFXP, fn("GLUTFXP"))

# -------- REPTDATE & macros --------
DF_R = pl.read_parquet(DEPOSIT_REPTDATE).with_columns(pl.col("REPTDATE").cast(pl.Date))
REPTDATE = DF_R.select("REPTDATE").row(0)[0]
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
RDATE    = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"  # DDMMYY8 text

# -------- GLFILE -> &GL (OBS=1) --------
DF_G = pl.read_parquet(GLFILE_PATH)
if "DATEX" not in DF_G.columns:
    raise SystemExit("ABORT 77: GLFILE missing DATEX (DDMMYY8.).")
gl_ddmmyy = DF_G.select(pl.col("DATEX").cast(pl.Utf8)).row(0)[0]
GL = gl_ddmmyy[:2] + gl_ddmmyy[2:4] + gl_ddmmyy[4:6]
if GL != RDATE:
    raise SystemExit(f"ABORT 77: THE GLIFLE EXTRACTION IS NOT DATED {RDATE}")

# -------- common detail (SIGN, DATE) --------
need = {"GLITEM","DATEX","BALANCE","SIGN"}
miss = need - set(DF_G.columns)
if miss: raise SystemExit(f"ABORT 77: GLFILE missing {sorted(miss)}.")
DETAIL = (
    DF_G
    .with_columns([
        pl.col("DATEX").cast(pl.Utf8).map_elements(ddmmyy8_to_date).alias("DATE"),
        pl.when(pl.col("SIGN") == "-").then(-pl.col("BALANCE")).otherwise(pl.col("BALANCE")).alias("BALANCE")
    ])
)

def build_pass(detail: pl.DataFrame, variant: int) -> pl.DataFrame:
    a221_or_a214 = "A2.21" if variant == 1 else "A2.14"
    df = (
        detail
        .with_columns([
            pl.when(pl.col("GLITEM").is_in(["49120","49120NLF"])).then(pl.lit("A1.20"))
             .when(pl.col("GLITEM").is_in(["F143120ODNCB","F143120ODNIB"])).then(pl.lit(a221_or_a214))
             .when(pl.col("GLITEM").is_in(["F13312002CB","F132121BBNM"])).then(pl.lit("A2.01"))
             .when(pl.col("GLITEM")=="37070").then(pl.lit("A2.08"))
             .otherwise(pl.lit(" ")).alias("ITEM"),
            pl.when(pl.col("GLITEM").is_in([
                "49120","49120NLF","F143120ODNCB","F143120ODNIB",
                "F13312002CB","F132121BBNM","37070"
            ])).then(pl.col("BALANCE")).otherwise(pl.lit(None)).alias("WEEK"),
        ])
        .with_columns(
            pl.sum_horizontal([
                pl.col("WEEK").fill_null(0.0),
                pl.col("MONTH").fill_null(0.0)  if "MONTH"  in detail.columns else pl.lit(0.0),
                pl.col("QTR").fill_null(0.0)    if "QTR"    in detail.columns else pl.lit(0.0),
                pl.col("HALFYR").fill_null(0.0) if "HALFYR" in detail.columns else pl.lit(0.0),
                pl.col("YEAR").fill_null(0.0)   if "YEAR"   in detail.columns else pl.lit(0.0),
                pl.col("LAST").fill_null(0.0)   if "LAST"   in detail.columns else pl.lit(0.0),
                pl.col("TOTAL").fill_null(0.0)  if "TOTAL"  in detail.columns else pl.lit(0.0),
            ]).alias("BALANCE")
        )
        .filter(pl.col("ITEM") != " ")
    )
    SUMV = ["WEEK","MONTH","QTR","HALFYR","YEAR","LAST","BALANCE"]
    for v in SUMV:
        df = df.with_columns(pl.col(v).fill_null(0.0)) if v in df.columns else df.with_columns(pl.lit(0.0).alias(v))
    agg = df.groupby("ITEM").agg([pl.col(v).sum().alias(v) for v in SUMV])
    return round_thousands(agg, SUMV)

# -------- Pass 1 (A2.21) --------
R1 = build_pass(DETAIL, variant=1)
split_and_write(R1, stub="1", y=REPTYEAR, m=REPTMON, d=REPTDAY)

# -------- Pass 2 (A2.14) --------
R2 = build_pass(DETAIL, variant=2)
split_and_write(R2, stub="2", y=REPTYEAR, m=REPTMON, d=REPTDAY)


now do the same for EIIMNLGL (islamic version)

  all inputs are in sas7bdat sas dataset. use pyreadstat to read remove reptdate, use datetime timedelta - 1 instead. output in sas7bdat and parquet files. write out using saspy

glfile is in text file
