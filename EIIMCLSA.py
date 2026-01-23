import polars as pl
from pathlib import Path
import datetime

INPUT_BASE_PATH = Path(r"C:\Your\Input\Path")
OUTPUT_BASE_PATH = Path(r"C:\Your\Output\Path")

PATHS = {
    "reptdate": INPUT_BASE_PATH / "DEP" / "REPTDATE.csv",
    "cis_deposit": INPUT_BASE_PATH / "CIS" / "DEPOSIT.csv",
    "savg_template": INPUT_BASE_PATH / "MIS" / "SAVGC{month}.csv",
    "crmsa_template": INPUT_BASE_PATH / "MNICRM" / "SA{month}.csv",
    "saacc": INPUT_BASE_PATH / "SIGNA" / "SMSACC.csv",
    "dpclos": INPUT_BASE_PATH / "PRODEV" / "DPCLOS.csv",
    "savg_mtd_template": INPUT_BASE_PATH / "MISMTD" / "SAVG{month}.csv",
}

def parse_sas_date(date_col: pl.Expr) -> pl.Expr:
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eiimclsa() -> pl.DataFrame:
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
    
    savg_path = str(PATHS["savg_template"]).format(month=REPTMON)
    if not Path(savg_path).exists():
        raise FileNotFoundError(f"SAVG file not found: {savg_path}")
    
    savg_df = pl.scan_csv(savg_path).sort("ACCTNO").collect()
    
    if REPTMON != REPTMON1 and REPTMON == "01":
        savg_df = savg_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        crmsa_path = str(PATHS["crmsa_template"]).format(month=REPTMON1)
        if Path(crmsa_path).exists():
            crmsa_df = (
                pl.scan_csv(crmsa_path)
                .unique("ACCTNO")
                .drop("COSTCTR")
                .collect()
            )
            savg_df = savg_df.join(crmsa_df, on="ACCTNO", how="inner")
    
    if PATHS["saacc"].exists():
        saacc_df = pl.scan_csv(PATHS["saacc"]).sort("ACCTNO").collect()
    else:
        saacc_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    isa_df = (
        cis_df.lazy()
        .join(savg_df.lazy(), on="ACCTNO", how="inner")
        .join(saacc_df.lazy(), on="ACCTNO", how="left")
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
    
    isaclose_df = isa_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
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
    
    savg_mtd_path = str(PATHS["savg_mtd_template"]).format(month=REPTMON)
    if Path(savg_mtd_path).exists():
        savg_mtd_df = pl.scan_csv(savg_mtd_path).collect()
    else:
        savg_mtd_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    final_df = (
        isaclose_df.lazy()
        .join(dpclos_df.lazy(), on="ACCTNO", how="left", suffix="_DPCLOS")
        .join(savg_mtd_df.lazy(), on="ACCTNO", how="left")
        .collect()
    )
    
    OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_BASE_PATH / f"ISACLOSE{REPTMON}{REPTYEAR}.parquet"
    
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result_df = process_eiimclsa()
