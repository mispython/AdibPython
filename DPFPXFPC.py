from __future__ import annotations
from pathlib import Path
from datetime import datetime, date
import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------------------
# Simple Paths
# ----------------------------
DEPOSIT_REPTDATE_PATH = Path("parquet_input/MNITB_DAILY_REPTDATE.parquet")   # DEPOSIT.REPTDATE
GLFILE_PATH           = Path("parquet_input/FDP_APPL_PIBB_DAILY_NLF.parquet")# GLFILE
STORE_DIR             = Path("parquet_output/PIBB_GL_NLF_DAILY")             # STORE.*
STORE_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------
def _fmt_DDMMYY8(d: date) -> str:
    # dd/mm/yy
    return d.strftime("%d/%m/%y")

def _ensure_date(v) -> date:
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        # try ISO first; if that fails, try dd/mm/yy
        try:
            return datetime.fromisoformat(v).date()
        except Exception:
            return datetime.strptime(v, "%d/%m/%y").date()
    raise ValueError("REPTDATE could not be parsed as a date")

def _parse_DDMMYY8(s: str) -> date:
    return datetime.strptime(s, "%d/%m/%y").date()

def _round_thousands_div_thousand(expr: pl.Expr) -> pl.Expr:
    # SAS: ROUND(x, 1000.) / 1000  ==> round to nearest 1000 then divide by 1000
    # This equals (x/1000).round(0)
    return (expr / 1000).round(0)

def _write_store(df: pl.DataFrame, name: str) -> None:
    out = STORE_DIR / f"{name}.parquet"
    df.write_parquet(out)

# ----------------------------
# 1) DATA REPTDATE + macros
# ----------------------------
df_rep = pl.read_parquet(DEPOSIT_REPTDATE_PATH)
if df_rep.height == 0:
    raise RuntimeError("DEPOSIT.REPTDATE is empty.")
REPTDATE: date = _ensure_date(df_rep.select("REPTDATE").row(0)[0])
REPTYEAR = REPTDATE.strftime("%Y")
REPTMON  = REPTDATE.strftime("%m")
REPTDAY  = REPTDATE.strftime("%d")
RDATE    = _fmt_DDMMYY8(REPTDATE)   # dd/mm/yy

# ----------------------------
# 2) Read GLFILE and derive GL date from first record
# ----------------------------
gl_df_raw = pl.read_parquet(GLFILE_PATH)

if gl_df_raw.height == 0:
    raise RuntimeError("GLFILE is empty.")

# Expect DATEX like dd/mm/yy
GLDATE = _parse_DDMMYY8(gl_df_raw.select("DATEX").row(0)[0])
GL = _fmt_DDMMYY8(GLDATE)

# ----------------------------
# %MACRO PROCESS gate: proceed only if GL == RDATE; else ABORT 77
# ----------------------------
if GL != RDATE:
    # Mimic: %PUT THE GLIFLE EXTRACTION IS NOT DATED &RDATE; ABORT 77;
    raise SystemExit(77)

# ----------------------------
# Common transformation for both passes (base columns)
# ----------------------------
def _prep_base(gl: pl.DataFrame) -> pl.DataFrame:
    # Mirror DATA step:
    # DATE = INPUT(DATEX, DDMMYY8.)
    # IF SIGN='-' THEN BALANCE = BALANCE*(-1)
    # Initialize WEEK, MONTH, QTR, HALFYR, YEAR, LAST, TOTAL to 0 (SAS SUM treats missing as 0)
    return (
        gl.with_columns(
            DATE = pl.col("DATEX").cast(pl.Utf8).map_elements(_parse_DDMMYY8),
            BALANCE = pl.when(pl.col("SIGN") == "-")
                        .then(pl.col("BALANCE") * -1)
                        .otherwise(pl.col("BALANCE")),
            WEEK    = pl.lit(0.0),
            MONTH   = pl.lit(0.0),
            QTR     = pl.lit(0.0),
            HALFYR  = pl.lit(0.0),
            YEAR    = pl.lit(0.0),
            LAST    = pl.lit(0.0),
            TOTAL   = pl.lit(0.0),
            ITEM    = pl.lit(""),
        )
    )

def _apply_mapping_pass1(df: pl.DataFrame) -> pl.DataFrame:
    # P1 mapping (A2.21)
    return (
        df.with_columns(
            ITEM = pl.when(pl.col("GLITEM").is_in(["49120", "49120NLF"]))
                       .then(pl.lit("A1.20"))
                 .when(pl.col("GLITEM").is_in(["F143120ODNCB", "F143120ODNIB"]))
                       .then(pl.lit("A2.21"))
                 .when(pl.col("GLITEM").is_in(["F13312002CB", "F132121BBNM"]))
                       .then(pl.lit("A2.01"))
                 .when(pl.col("GLITEM") == "37070")
                       .then(pl.lit("A2.08"))
                 .otherwise(pl.lit("")),
            WEEK = pl.when(
                        pl.col("GLITEM").is_in(
                            ["49120","49120NLF","F143120ODNCB","F143120ODNIB",
                             "F13312002CB","F132121BBNM","37070"]
                        )
                    ).then(pl.col("BALANCE"))
                     .otherwise(pl.col("WEEK"))
        )
        .with_columns(
            BALANCE = pl.col("WEEK") + pl.col("MONTH") + pl.col("QTR") +
                      pl.col("HALFYR") + pl.col("YEAR") + pl.col("LAST") + pl.col("TOTAL")
        )
        .filter(pl.col("ITEM") != "")
    )

def _apply_mapping_pass2(df: pl.DataFrame) -> pl.DataFrame:
    # P2 mapping (A2.14)
    return (
        df.with_columns(
            ITEM = pl.when(pl.col("GLITEM").is_in(["49120", "49120NLF"]))
                       .then(pl.lit("A1.20"))
                 .when(pl.col("GLITEM").is_in(["F143120ODNCB", "F143120ODNIB"]))
                       .then(pl.lit("A2.14"))
                 .when(pl.col("GLITEM").is_in(["F13312002CB", "F132121BBNM"]))
                       .then(pl.lit("A2.01"))
                 .when(pl.col("GLITEM") == "37070")
                       .then(pl.lit("A2.08"))
                 .otherwise(pl.lit("")),
            WEEK = pl.when(
                        pl.col("GLITEM").is_in(
                            ["49120","49120NLF","F143120ODNCB","F143120ODNIB",
                             "F13312002CB","F132121BBNM","37070"]
                        )
                    ).then(pl.col("BALANCE"))
                     .otherwise(pl.col("WEEK"))
        )
        .with_columns(
            BALANCE = pl.col("WEEK") + pl.col("MONTH") + pl.col("QTR") +
                      pl.col("HALFYR") + pl.col("YEAR") + pl.col("LAST") + pl.col("TOTAL")
        )
        .filter(pl.col("ITEM") != "")
    )

def _summary_by_item(df: pl.DataFrame) -> pl.DataFrame:
    # PROC SUMMARY NWAY; CLASS ITEM; VAR WEEK MONTH QTR HALFYR YEAR LAST BALANCE; OUTPUT SUM=;
    return (
        df.group_by("ITEM")
          .agg([
              pl.col("WEEK").sum().alias("WEEK"),
              pl.col("MONTH").sum().alias("MONTH"),
              pl.col("QTR").sum().alias("QTR"),
              pl.col("HALFYR").sum().alias("HALFYR"),
              pl.col("YEAR").sum().alias("YEAR"),
              pl.col("LAST").sum().alias("LAST"),
              pl.col("BALANCE").sum().alias("BALANCE"),
          ])
          .sort("ITEM")
    )

def _apply_rounding_and_split(df: pl.DataFrame, pass_label: str) -> None:
    """
    Mirrors:
      WEEK    = ROUND(WEEK,1000.)/1000;
      ...
      BALANCE = ROUND(BALANCE,1000.)/1000;
      IF SUBSTR(ITEM,1,1)='A' THEN DO;
         IF SUBSTR(ITEM,2,1)='1' THEN OUTPUT STORE.GLRMP{pass}
         IF SUBSTR(ITEM,2,1)='2' THEN OUTPUT STORE.GLUTRMP{pass}
      END;
      ELSE DO;
         IF SUBSTR(ITEM,2,1)='1' THEN OUTPUT STORE.GLFXP{pass}
         IF SUBSTR(ITEM,2,1)='2' THEN OUTPUT STORE.GLUTFXP{pass}
      END;
    """
    rounded = (
        df.with_columns(
            WEEK    = _round_thousands_div_thousand(pl.col("WEEK")),
            MONTH   = _round_thousands_div_thousand(pl.col("MONTH")),
            QTR     = _round_thousands_div_thousand(pl.col("QTR")),
            HALFYR  = _round_thousands_div_thousand(pl.col("HALFYR")),
            YEAR    = _round_thousands_div_thousand(pl.col("YEAR")),
            LAST    = _round_thousands_div_thousand(pl.col("LAST")),
            BALANCE = _round_thousands_div_thousand(pl.col("BALANCE")),
        )
    )

    # Create selectors
    first1 = rounded["ITEM"].str.slice(0, 1)
    second1 = rounded["ITEM"].str.slice(1, 1)

    # Partitions
    A_mask   = first1 == "A"
    notA     = first1 != "A"
    sec_is_1 = second1 == "1"
    sec_is_2 = second1 == "2"

    YYYYMMDD = f"{REPTYEAR}{REPTMON}{REPTDAY}"

    GLRMP  = rounded.filter(A_mask & sec_is_1)
    GLUTRM = rounded.filter(A_mask & sec_is_2)
    GLFXP  = rounded.filter(notA  & sec_is_1)
    GLUTFX = rounded.filter(notA  & sec_is_2)

    _write_store(GLRMP,  f"GLRMP{pass_label}{YYYYMMDD}")
    _write_store(GLFXP,  f"GLFXP{pass_label}{YYYYMMDD}")
    _write_store(GLUTRM, f"GLUTRMP{pass_label}{YYYYMMDD}")
    _write_store(GLUTFX, f"GLUTFXP{pass_label}{YYYYMMDD}")

# ----------------------------
# Build both passes
# ----------------------------
base = _prep_base(gl_df_raw)

# Pass 1
p1 = _apply_mapping_pass1(base)
p1_sum = _summary_by_item(p1)
_apply_rounding_and_split(p1_sum, pass_label="1")

# Pass 2
p2 = _apply_mapping_pass2(base)
p2_sum = _summary_by_item(p2)
_apply_rounding_and_split(p2_sum, pass_label="2")

# (PROC PRINT equivalents omitted—files are written as Parquet.)
