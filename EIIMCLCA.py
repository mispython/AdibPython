import polars as pl
from pathlib import Path
import datetime
from typing import Optional

INPUT_BASE_PATH = Path(r"C:\Your\Input\Path")
OUTPUT_BASE_PATH = Path(r"C:\Your\Output\Path")

PATHS = {
    "reptdate": INPUT_BASE_PATH / "DEP" / "REPTDATE.csv",
    "cis_deposit": INPUT_BASE_PATH / "CIS" / "DEPOSIT.csv",
    "curr_template": INPUT_BASE_PATH / "MIS" / "CURRC{month}.csv",
    "crmca_template": INPUT_BASE_PATH / "MNICRM" / "CA{month}.csv",
    "dpclos": INPUT_BASE_PATH / "PRODEV" / "DPCLOS.csv",
    "accum": INPUT_BASE_PATH / "DRCRCA" / "ACCUM.csv",
    "saacc": INPUT_BASE_PATH / "SIGNA" / "SMSACC.csv",
    "cavg_template": INPUT_BASE_PATH / "MISMTD" / "CAVG{month}.csv",
}

def parse_sas_date(date_col: pl.Expr) -> pl.Expr:
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eiimclca(nowk: str = "4") -> pl.DataFrame:
    reptdate_df = pl.read_csv(PATHS["reptdate"])
    reptdate_str = reptdate_df["REPTDATE"][0]
    reptdate = None
    
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d", "%d%m%Y"]
    for fmt in date_formats:
        try:
            reptdate = datetime.datetime.strptime(str(reptdate_str), fmt).date()
            break
        except ValueError:
            continue
    
    if not reptdate:
        raise ValueError(f"Cannot parse REPTDATE: {reptdate_str}")
    
    mm = reptdate.month
    next_day = reptdate + datetime.timedelta(days=1)
    mm1 = mm if next_day.day == 1 else mm - 1
    if mm1 == 0:
        mm1 = 12
    
    REPTYEAR = str(reptdate.year)[-2:]
    REPTMON = f"{mm:02d}"
    REPTMON1 = f"{mm1:02d}"
    
    cis_df = (
        pl.scan_csv(PATHS["cis_deposit"])
        .filter(pl.col("SECCUST") == "901")
        .with_columns([
            pl.col("CITIZEN").alias("COUNTRY"),
            pl.col("BIRTHDAT").str.slice(0, 2).cast(pl.Int32).alias("BDD"),
            pl.col("BIRTHDAT").str.slice(2, 2).cast(pl.Int32).alias("BMM"),
            pl.col("BIRTHDAT").str.slice(4, 4).cast(pl.Int32).alias("BYY")
        ])
        .with_columns([
            pl.date(pl.col("BYY"), pl.col("BMM"), pl.col("BDD")).alias("DOBCIS")
        ])
        .select(["ACCTNO", "DOBCIS", "OCCUPAT", "COUNTRY", "RACE"])
        .unique("ACCTNO")
        .collect()
    )
    
    curr_path = str(PATHS["curr_template"]).format(month=REPTMON)
    if not Path(curr_path).exists():
        raise FileNotFoundError(f"CURR file not found: {curr_path}")
    
    curr_df = pl.scan_csv(curr_path).sort("ACCTNO").collect()
    
    if nowk != "4" and REPTMON == "01":
        curr_df = curr_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        crmca_path = str(PATHS["crmca_template"]).format(month=REPTMON1)
        if Path(crmca_path).exists():
            crmca_df = (
                pl.scan_csv(crmca_path)
                .unique("ACCTNO")
                .drop(["COSTCTR", "FEEPD"])
                .collect()
            )
            curr_df = curr_df.join(crmca_df, on="ACCTNO", how="inner")
    
    ica_df = (
        cis_df.lazy()
        .join(curr_df.lazy(), on="ACCTNO", how="inner")
        .with_columns([
            pl.when(pl.col("OPENDT") > 0)
            .then(parse_sas_date(pl.col("OPENDT")))
            .alias("ODATE"),
            pl.when(pl.col("CLOSEDT") > 0)
            .then(parse_sas_date(pl.col("CLOSEDT")))
            .alias("CDATE")
        ])
        .drop(["OPENDT", "CLOSEDT"])
        .collect()
    )
    
    icaclose_df = ica_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT"),
        pl.col("RISKCODE").cast(pl.Utf8).str.slice(0, 1).alias("RISKRATE")
    ]).drop(["ODATE", "CDATE"])
    
    if PATHS["dpclos"].exists():
        dpclos_df = (
            pl.scan_csv(PATHS["dpclos"])
            .sort("CLOSEDT", descending=True)
            .unique("ACCTNO", keep="first")
            .select(["ACCTNO", "CLOSEDT", "RCODE"])
            .sort("ACCTNO")
            .collect()
        )
    else:
        dpclos_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "CLOSEDT": pl.Date, "RCODE": pl.Utf8})
    
    accum_df = (
        pl.scan_csv(PATHS["accum"]).sort("ACCTNO").collect() 
        if PATHS["accum"].exists() 
        else pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    )
    
    saacc_df = (
        pl.scan_csv(PATHS["saacc"]).sort("ACCTNO").collect() 
        if PATHS["saacc"].exists() 
        else pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    )
    
    cavg_path = str(PATHS["cavg_template"]).format(month=REPTMON)
    if Path(cavg_path).exists():
        cavg_df = pl.scan_csv(cavg_path).select(["ACCTNO", "MTDAVBAL_MIS"]).collect()
    else:
        cavg_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "MTDAVBAL_MIS": pl.Float64})
    
    final_df = icaclose_df
    
    datasets = [
        (dpclos_df, "ACCTNO", "left", "_DPCLOS"),
        (accum_df, "ACCTNO", "left", ""),
        (saacc_df, "ACCTNO", "left", ""),
        (cavg_df, "ACCTNO", "left", "")
    ]
    
    for df, on, how, suffix in datasets:
        if len(df) > 0:
            final_df = final_df.join(df, on=on, how=how, suffix=suffix)
    
    final_df = final_df.with_columns([
        pl.when(pl.col("ESIGNATURE") == "")
        .then(pl.lit("N"))
        .otherwise(pl.col("ESIGNATURE"))
        .alias("ESIGNATURE")
    ])
    
    OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_BASE_PATH / f"ICACLOSE{REPTMON}{REPTYEAR}.parquet"
    
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result_df = process_eiimclca(nowk="4")
