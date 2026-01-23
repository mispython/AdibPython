import polars as pl
from pathlib import Path
import datetime

BASE_INPUT = Path("C:/Your/Base/Input/Path")
BASE_OUTPUT = Path("C:/Your/Base/Output/Path")

DEP_REPTDATE = BASE_INPUT / "DEP" / "REPTDATE.csv"
CIS_DEPOSIT = BASE_INPUT / "CIS" / "DEPOSIT.csv"
SIGNA_SMSACC = BASE_INPUT / "SIGNA" / "SMSACC.csv"

def parse_sas_date(date_col):
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eibmclfd():
    reptdate_df = pl.read_csv(DEP_REPTDATE)
    reptdate_str = reptdate_df["REPTDATE"][0]
    
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d", "%d%m%Y"]:
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
        pl.scan_csv(CIS_DEPOSIT)
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
    
    fd_path = BASE_INPUT / "MIS" / f"FDC{REPTMON}.csv"
    if not fd_path.exists():
        raise FileNotFoundError(f"FD file not found: {fd_path}")
    
    fd_df = pl.scan_csv(fd_path).sort("ACCTNO").collect()
    
    crmfd_path = BASE_INPUT / "MNICRM" / f"FD{REPTMON}.csv"
    if crmfd_path.exists():
        crmfd_df = (
            pl.scan_csv(crmfd_path)
            .unique("ACCTNO")
            .drop(["COSTCTR", "FEEPD"])
            .collect()
        )
        fd_df = fd_df.join(crmfd_df, on="ACCTNO", how="inner")
    
    if SIGNA_SMSACC.exists():
        saacc_df = pl.scan_csv(SIGNA_SMSACC).sort("ACCTNO").collect()
    else:
        saacc_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    favg_path = BASE_INPUT / "MISMTD" / f"FAVG{REPTMON}.csv"
    if favg_path.exists():
        favg_df = pl.scan_csv(favg_path).select(["ACCTNO", "MTDAVBAL_MIS"]).collect()
    else:
        favg_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "MTDAVBAL_MIS": pl.Float64})
    
    ddwh_df = (
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
    
    final_df = ddwh_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
    ]).drop(["ODATE", "CDATE"])
    
    BASE_OUTPUT.mkdir(parents=True, exist_ok=True)
    output_file = BASE_OUTPUT / f"FDCLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result = process_eibmclfd()
