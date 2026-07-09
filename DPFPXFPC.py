
from __future__ import annotations
from pathlib import Path
from datetime import date
import polars as pl
import pyarrow.parquet as pq
import duckdb  # optional (kept for your stack consistency)

# ---- paths (parquet) ----
BASE = Path("Data_Warehouse")
DEPOSIT_REPTDATE = BASE / "SAP.PBB.MNITB.DAILY" / "REPTDATE.parquet"
GLFILE_PATH      = BASE / "FDP.APPL.PIVB.DAILY.NLF.parquet"
STORE_OUT        = BASE / "SAP.PIVB.GL.NLF.DAILY"
STORE_OUT.mkdir(parents=True, exist_ok=True)

# ---- read REPTDATE + macros ----
DF_R = pl.read_parquet(DEPOSIT_REPTDATE).with_columns(pl.col("REPTDATE").cast(pl.Date))
REPTDATE = DF_R.select("REPTDATE").row(0)[0]
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
RDATE    = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"  # DDMMYY8. text (ddmmyy)

# ---- read GLFILE, get GL macro from first row ----
DF_G = pl.read_parquet(GLFILE_PATH)

def ddmmyy8_to_date(s: str) -> date:
    dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
    yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
    return date(yy, mm, dd)

if "DATEX" in DF_G.columns:
    gl_ddmmyy = DF_G.select(pl.col("DATEX").cast(pl.Utf8)).row(0)[0]
    GL = gl_ddmmyy[:2] + gl_ddmmyy[2:4] + gl_ddmmyy[4:6]  # ddmmyy text
else:
    raise SystemExit("ABORT 77: GLFILE missing DATEX (DDMMYY8.).")

# ---- main logic ----
if GL == RDATE:
    # detail step
    need = {"GLITEM", "DATEX", "BALANCE", "SIGN"}
    miss = need - set(DF_G.columns)
    if miss:
        raise SystemExit(f"ABORT 77: GLFILE missing {sorted(miss)}.")

    GLFILEP1 = (
        DF_G
        .with_columns([
            pl.col("DATEX").cast(pl.Utf8)
              .map_elements(lambda s: ddmmyy8_to_date(s)).alias("DATE"),
            pl.when(pl.col("SIGN") == "-")
              .then(-pl.col("BALANCE"))
              .otherwise(pl.col("BALANCE"))
              .alias("BALANCE"),
        ])
        # Flip positive BALANCE to negative for specific GLITEM values
        .with_columns(
            pl.when(
                pl.col("GLITEM").is_in(["S-RCF", "S-GUARANTEE", "S-SM F", "S-TLF", "S-BA F"])
            ).then(
                pl.when(pl.col("BALANCE") > 0, -pl.col("BALANCE")).otherwise(pl.col("BALANCE"))
            ).otherwise(pl.col("BALANCE"))
            .alias("BALANCE")
        )
        # ITEM + WEEK/MONTH
        .with_columns([
            pl.when(pl.col("GLITEM") == "S-RCF").then(pl.lit("A1.35"))
             .when(pl.col("GLITEM") == "S-GUARANTEE").then(pl.lit("A1.36"))
             .when(pl.col("GLITEM") == "S-SM F").then(pl.lit("A1.37"))
             .when(pl.col("GLITEM").is_in(["S-TLF", "S-BA F"])).then(pl.lit("A1.38"))
             .when(pl.col("GLITEM").is_in(["S-FIXED DEP", "S-REMISIERFD"])).then(pl.lit("A2.01"))
             .otherwise(pl.lit(" ")).alias("ITEM"),
            pl.when(pl.col("GLITEM").is_in(["S-RCF", "S-GUARANTEE", "S-SM F", "S-TLF", "S-BA F"]))
              .then(pl.col("BALANCE") * 0.2).otherwise(pl.lit(None)).alias("WEEK"),
            pl.when(pl.col("GLITEM").is_in(["S-FIXED DEP", "S-REMISIERFD"]))
              .then(pl.col("BALANCE")).otherwise(pl.lit(None)).alias("MONTH"),
        ])
        # BALANCE = SUM(WEEK,MONTH,QTR,HALFYR,YEAR,LAST,TOTAL) (missing -> 0)
        .with_columns(
            pl.sum_horizontal([
                pl.col("WEEK").fill_null(0.0),
                pl.col("MONTH").fill_null(0.0),
                pl.col("QTR").fill_null(0.0) if "QTR" in DF_G.columns else pl.lit(0.0),
                pl.col("HALFYR").fill_null(0.0) if "HALFYR" in DF_G.columns else pl.lit(0.0),
                pl.col("YEAR").fill_null(0.0) if "YEAR" in DF_G.columns else pl.lit(0.0),
                pl.col("LAST").fill_null(0.0) if "LAST" in DF_G.columns else pl.lit(0.0),
                pl.col("TOTAL").fill_null(0.0) if "TOTAL" in DF_G.columns else pl.lit(0.0),
            ]).alias("BALANCE")
        )
        .filter(pl.col("ITEM") != " ")
    )

    # summary (NWAY sum by ITEM)
    SUMV = ["WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE"]
    for v in SUMV:
        if v not in GLFILEP1.columns:
            GLFILEP1 = GLFILEP1.with_columns(pl.lit(0.0).alias(v))
        else:
            GLFILEP1 = GLFILEP1.with_columns(pl.col(v).fill_null(0.0).alias(v))

    GL_SUM = GLFILEP1.groupby("ITEM").agg([pl.col(v).sum().alias(v) for v in SUMV])

    # rounding: ROUND(x,1000.)/1000 → round to nearest thousand, then scale back
    R = GL_SUM.with_columns([
        ((pl.col(c) / 1000).round(0) / 1000).alias(c) for c in SUMV
    ])

    # split & write (use helper cols to avoid length mismatches)
    def fname(stub: str) -> Path:
        return STORE_OUT / f"{stub}{REPTYEAR}{REPTMON}{REPTDAY}.parquet"

    A = (
        R.with_columns([
            pl.col("ITEM").str.slice(0, 1).alias("S1"),
            pl.col("ITEM").str.slice(1, 1).alias("S2"),
        ])
        .filter(pl.col("S1") == "A")
    )
    NA = (
        R.with_columns([
            pl.col("ITEM").str.slice(0, 1).alias("S1"),
            pl.col("ITEM").str.slice(1, 1).alias("S2"),
        ])
        .filter(pl.col("S1") != "A")
    )

    GLRMP1   = A.filter(pl.col("S2") == "1").drop(["S1", "S2"])
    GLUTRMP1 = A.filter(pl.col("S2") == "2").drop(["S1", "S2"])
    GLFXP1   = NA.filter(pl.col("S2") == "1").drop(["S1", "S2"])
    GLUTFXP1 = NA.filter(pl.col("S2") == "2").drop(["S1", "S2"])

    pq.write_table(GLRMP1.to_arrow(),   fname("GLRMP1"))
    pq.write_table(GLFXP1.to_arrow(),   fname("GLFXP1"))
    pq.write_table(GLUTRMP1.to_arrow(), fname("GLUTRMP1"))
    pq.write_table(GLUTFXP1.to_arrow(), fname("GLUTFXP1"))

else:
    raise SystemExit(f"ABORT 77: THE GLIFLE EXTRACTION IS NOT DATED {RDATE}")
