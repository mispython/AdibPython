import polars as pl
from pathlib import Path
import datetime

INPUT_BASE_PATH = Path(r"C:\Your\Input\Path")
OUTPUT_BASE_PATH = Path(r"C:\Your\Output\Path")

PATHS = {
    "reptdate": INPUT_BASE_PATH / "DEP" / "REPTDATE.csv",
    "cis_deposit": INPUT_BASE_PATH / "CIS" / "DEPOSIT.csv",
    "fd_template": INPUT_BASE_PATH / "MIS" / "FDC{month}.csv",
    "crmfd_template": INPUT_BASE_PATH / "MNICRM" / "FD{month}.csv",
    "saacc": INPUT_BASE_PATH / "SIGNA" / "SMSACC.csv",
    "favg_template": INPUT_BASE_PATH / "MISMTD" / "FAVG{month}.csv",
}

def parse_sas_date(date_col: pl.Expr) -> pl.Expr:
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eiimclfd() -> pl.DataFrame:
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
    
    REPTYEAR = str(reptdate.year)[-2:]
    REPTMON = f"{reptdate.month:02d}"
    
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
    
    fd_path = str(PATHS["fd_template"]).format(month=REPTMON)
    if not Path(fd_path).exists():
        raise FileNotFoundError(f"FD file not found: {fd_path}")
    
    fd_df = pl.scan_csv(fd_path).sort("ACCTNO").collect()
    
    crmfd_path = str(PATHS["crmfd_template"]).format(month=REPTMON)
    if Path(crmfd_path).exists():
        crmfd_df = (
            pl.scan_csv(crmfd_path)
            .unique("ACCTNO")
            .drop(["COSTCTR", "FEEPD"])
            .collect()
        )
        fd_df = fd_df.join(crmfd_df, on="ACCTNO", how="inner")
    
    if PATHS["saacc"].exists():
        saacc_df = pl.scan_csv(PATHS["saacc"]).sort("ACCTNO").collect()
    else:
        saacc_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    favg_path = str(PATHS["favg_template"]).format(month=REPTMON)
    if Path(favg_path).exists():
        favg_df = pl.scan_csv(favg_path).select(["ACCTNO", "MTDAVBAL_MIS"]).collect()
    else:
        favg_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "MTDAVBAL_MIS": pl.Float64})
    
    ifd_df = (
        cis_df.lazy()
        .join(fd_df.lazy(), on="ACCTNO", how="inner")
        .join(saacc_df.lazy(), on="ACCTNO", how="left")
        .join(favg_df.lazy(), on="ACCTNO", how="left")
        .with_columns([
            pl.when(pl.col("OPENDT") > 0)
            .then(parse_sas_date(pl.col("OPENDT")))
            .alias("ODATE"),
            pl.when(pl.col("CLOSEDT") > 0)
            .then(parse_sas_date(pl.col("CLOSEDT")))
            .alias("CDATE"),
            pl.when(pl.col("ESIGNATURE") == "")
            .then(pl.lit("N"))
            .otherwise(pl.col("ESIGNATURE"))
            .alias("ESIGNATURE")
        ])
        .drop(["OPENDT", "CLOSEDT"])
        .collect()
    )
    
    final_df = ifd_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
    ]).drop(["ODATE", "CDATE"])
    
    OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_BASE_PATH / f"IFDCLOSE{REPTMON}{REPTYEAR}.parquet"
    
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result_df = process_eiimclfd()
