from pathlib import Path
from datetime import datetime, timedelta
import polars as pl

BASE_IN  = Path("input_parquet")
BASE_OUT = Path("output_parquet")
(BASE_OUT / "BNM").mkdir(parents=True, exist_ok=True)

def _derive_REPTDATE_from_TBDATE(df: pl.DataFrame) -> pl.Date:
    """
    SAS does: PUT(TBDATE, Z11.) -> take first 8 chars -> INPUT(..., MMDDYY8.)
    We mirror that exactly:
      - zero-pad TBDATE to width 11 (as integer/string)
      - take [:8] as MMDDYYYY
    """
    if df.height == 0:
        raise ValueError("Empty DPTFL2* source; cannot derive REPTDATE.")
    tbd = df.select("TBDATE").to_series().drop_nans().drop_nulls().item(0)
    s   = str(int(tbd)).zfill(11)[:8]  # MMDDYYYY
    dt  = datetime.strptime(s, "%m%d%Y").date()
    return dt

def write_RPTDATE(I: str, dpt: pl.DataFrame):
    reptdate = _derive_REPTDATE_from_TBDATE(dpt)
    pl.DataFrame({"REPTDATE":[reptdate]}).write_parquet(BASE_OUT / "BNM" / f"RPTDATE{I}.parquet")

# ===== [Python/Polars] TELLER(I) equivalent =====
def build_TELLER(I: str) -> pl.DataFrame:
    dpt = pl.read_parquet(BASE_IN / f"DPTFL2{I}.parquet")
    # RPTDATE (from first record TBDATE) exactly as SAS does
    reptdate = _derive_REPTDATE_from_TBDATE(dpt)
    # Filter rows equivalent to REPTNO=210 & FMTCODE=1, then select/rename columns
    teller = (
        dpt
        .filter((pl.col("REPTNO")==210) & (pl.col("FMTCODE")==1))
        .select([
            pl.col("BRANCH").alias("BRANCH"),
            pl.col("AMOUNT").alias("AMOUNT"),
            pl.col("TRANCODE").cast(pl.Utf8).alias("TRANCODE"),
            pl.col("TRANNAME").cast(pl.Utf8).alias("TRANNAME"),
            pl.lit(reptdate).alias("REPTDATE"),
            pl.col("CASHIN").alias("CASHIN"),
            pl.col("CASHOUT").alias("CASHOUT"),
            pl.col("CHECKIN").alias("CHECKIN"),
        ])
        .sort(["BRANCH","TRANCODE"])
    )
    return teller

# ===== [Python/Polars] %TELLER1(I) aggregation =====
def teller1_agg(teller: pl.DataFrame) -> pl.DataFrame:
    ABSAMT = pl.col("AMOUNT").abs()
    return (
        teller
        .group_by(["BRANCH","TRANCODE","TRANNAME","REPTDATE"])
        .agg([
            pl.col("AMOUNT").sum().alias("AMT"),
            pl.col("CASHIN").sum().alias("CSHIN"),
            pl.col("CASHOUT").sum().alias("CSHOUT"),
            pl.col("CHECKIN").sum().alias("CHEQUE"),
            ( (ABSAMT>=0) & (ABSAMT<=5_000) ).cast(pl.Int64).sum().alias("ITEM1"),
            ( (ABSAMT>5_000) & (ABSAMT<=10_000) ).cast(pl.Int64).sum().alias("ITEM2"),
            ( (ABSAMT>10_000) & (ABSAMT<=50_000) ).cast(pl.Int64).sum().alias("ITEM3"),
            ( (ABSAMT>50_000) & (ABSAMT<=100_000) ).cast(pl.Int64).sum().alias("ITEM4"),
            ( (ABSAMT>100_000) & (ABSAMT<=200_000) ).cast(pl.Int64).sum().alias("ITEM5"),
            ( (ABSAMT>200_000) ).cast(pl.Int64).sum().alias("ITEM6"),
        ])
        .select(["BRANCH","TRANCODE","TRANNAME","AMT","CSHIN","CSHOUT","CHEQUE",
                 "REPTDATE","ITEM1","ITEM2","ITEM3","ITEM4","ITEM5","ITEM6"])
    )

# ===== [Python/Polars] %TELLER2(I) aggregation with 14 sub-buckets =====
def teller2_agg(teller: pl.DataFrame) -> pl.DataFrame:
    ABSAMT = pl.col("AMOUNT").abs()
    return (
        teller
        .group_by(["BRANCH","TRANCODE","TRANNAME","REPTDATE"])
        .agg([
            pl.col("AMOUNT").sum().alias("AMT"),
            pl.col("CASHIN").sum().alias("CSHIN"),
            pl.col("CASHOUT").sum().alias("CSHOUT"),
            pl.col("CHECKIN").sum().alias("CHEQUE"),
            # ITEM1..ITEM6 (same bands as SAS)
            ( (ABSAMT>=0) & (ABSAMT<=5_000) ).cast(pl.Int64).sum().alias("ITEM1"),
            ( (ABSAMT>5_000) & (ABSAMT<=10_000) ).cast(pl.Int64).sum().alias("ITEM2"),
            ( (ABSAMT>10_000) & (ABSAMT<=50_000) ).cast(pl.Int64).sum().alias("ITEM3"),
            ( (ABSAMT>50_000) & (ABSAMT<=100_000) ).cast(pl.Int64).sum().alias("ITEM4"),
            ( (ABSAMT>100_000) & (ABSAMT<=200_000) ).cast(pl.Int64).sum().alias("ITEM5"),
            ( (ABSAMT>200_000) ).cast(pl.Int64).sum().alias("ITEM6"),
            # COUNT1..COUNT14 sub-bands:
            ( (ABSAMT>=0) & (ABSAMT<=3_000) ).cast(pl.Int64).sum().alias("COUNT1"),
            ( (ABSAMT>3_000) & (ABSAMT<=5_000) ).cast(pl.Int64).sum().alias("COUNT2"),
            ( (ABSAMT>5_000) & (ABSAMT<=10_000) ).cast(pl.Int64).sum().alias("COUNT3"),
            ( (ABSAMT>10_000) & (ABSAMT<=15_000) ).cast(pl.Int64).sum().alias("COUNT4"),
            ( (ABSAMT>15_000) & (ABSAMT<=20_000) ).cast(pl.Int64).sum().alias("COUNT5"),
            ( (ABSAMT>20_000) & (ABSAMT<=25_000) ).cast(pl.Int64).sum().alias("COUNT6"),
            ( (ABSAMT>25_000) & (ABSAMT<=30_000) ).cast(pl.Int64).sum().alias("COUNT7"),
            ( (ABSAMT>30_000) & (ABSAMT<=35_000) ).cast(pl.Int64).sum().alias("COUNT8"),
            ( (ABSAMT>35_000) & (ABSAMT<=40_000) ).cast(pl.Int64).sum().alias("COUNT9"),
            ( (ABSAMT>40_000) & (ABSAMT<=45_000) ).cast(pl.Int64).sum().alias("COUNT10"),
            ( (ABSAMT>45_000) & (ABSAMT<=50_000) ).cast(pl.Int64).sum().alias("COUNT11"),
            ( (ABSAMT>50_000) & (ABSAMT<=100_000) ).cast(pl.Int64).sum().alias("COUNT12"),
            ( (ABSAMT>100_000) & (ABSAMT<=200_000) ).cast(pl.Int64).sum().alias("COUNT13"),
            ( (ABSAMT>200_000) ).cast(pl.Int64).sum().alias("COUNT14"),
        ])
        .select(["BRANCH","TRANCODE","TRANNAME","AMT","CSHIN","CSHOUT","CHEQUE",
                 "REPTDATE","ITEM1","ITEM2","ITEM3","ITEM4","ITEM5","ITEM6",
                 "COUNT1","COUNT2","COUNT3","COUNT4","COUNT5","COUNT6","COUNT7",
                 "COUNT8","COUNT9","COUNT10","COUNT11","COUNT12","COUNT13","COUNT14"])
    )

# ===== [Python/Polars] APPEND(I) monthly accumulation & rollover =====
def _read_or_empty(path: Path, schema_like: pl.DataFrame|None=None) -> pl.DataFrame:
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema_like.schema) if schema_like is not None else pl.DataFrame()

def append_month(I: str, TELLER_df: pl.DataFrame):
    rpt = pl.read_parquet(BASE_OUT / "BNM" / f"RPTDATE{I}.parquet")
    REPTDATE = rpt.select(pl.col("REPTDATE")).to_series().item()

    RDAY = REPTDATE.day
    LDAY = (REPTDATE + timedelta(days=1)).day

    mn_path     = BASE_OUT / "BNM" / f"MNITLR{I}.parquet"
    mn1_path    = BASE_OUT / "BNM" / f"MNITLR1{I}.parquet"
    bkupd_path  = BASE_OUT / "BNM" / f"BKUPDTE{I}.parquet"

    # If day 1 of month: start fresh (delete MNITLR{I})
    if RDAY == 1 and mn_path.exists():
        mn_path.unlink()

    # Remove any existing rows for current RDAY (SAS deletes same-day rows before append)
    MN = _read_or_empty(mn_path, schema_like=TELLER_df)
    if MN.height:
        MN = MN.filter(pl.col("REPTDATE").dt.day() != RDAY)

    # Append today’s TELLER
    MN = pl.concat([MN, TELLER_df], how="vertical", rechunk=True)
    MN.write_parquet(mn_path)

    # Month end rollover when next day is 1 (LDAY==1)
    if LDAY == 1:
        # SAS PROC DATASETS AGE creates MNITLR1 as the “aged” copy; we mirror by copying MN -> MNITLR1 then replacing MNITLR with that copy
        MN.write_parquet(mn1_path)
        pl.read_parquet(mn1_path).write_parquet(mn_path)
        pl.DataFrame({"REPTDATE":[REPTDATE]}).write_parquet(bkupd_path)

# ===== [Python/Polars] Driver =====
def run_for(I: str, use_teller2: bool):
    src = pl.read_parquet(BASE_IN / f"DPTFL2{I}.parquet")
    write_RPTDATE(I, src)
    TELL = build_TELLER(I)
    TELL_AGG = teller2_agg(TELL) if use_teller2 else teller1_agg(TELL)
    # Persist the day’s aggregated TELLER (optional but handy for debug)
    (BASE_OUT / "BNM").mkdir(parents=True, exist_ok=True)
    TELL_AGG.write_parquet(BASE_OUT / "BNM" / f"TELLER_{I}.parquet")
    append_month(I, TELL_AGG)

if __name__ == "__main__":
    run_for("B", use_teller2=True)   # PBB portion uses %TELLER2(B)
    run_for("F", use_teller2=False)  # PFB portion uses %TELLER1(F)

