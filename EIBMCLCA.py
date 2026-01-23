import polars as pl
from pathlib import Path
import datetime

BASE_INPUT = Path("C:/Your/Base/Input/Path")
BASE_OUTPUT = Path("C:/Your/Base/Output/Path")

DEP_REPTDATE = BASE_INPUT / "DEP" / "REPTDATE.csv"
CIS_DEPOSIT = BASE_INPUT / "CIS" / "DEPOSIT.csv"
SIGNA_SMSACC = BASE_INPUT / "SIGNA" / "SMSACC.csv"
PRODEV_DPCLOS = BASE_INPUT / "PRODEV" / "DPCLOS.csv"
DRCRCA_ACCUM = BASE_INPUT / "DRCRCA" / "ACCUM.csv"

def parse_sas_date(date_col):
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eibmclca(nowk="4"):
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
    
    mm = reptdate.month
    next_day = reptdate + datetime.timedelta(days=1)
    mm1 = mm if next_day.day == 1 else mm - 1
    if mm1 == 0:
        mm1 = 12
    
    REPTYEAR = str(reptdate.year)[-2:]
    REPTMON = f"{mm:02d}"
    REPTMON1 = f"{mm1:02d}"
    
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
    
    curr_path = BASE_INPUT / "MIS" / f"CURRC{REPTMON}.csv"
    if not curr_path.exists():
        raise FileNotFoundError(f"CURR file not found: {curr_path}")
    
    curr_df = pl.scan_csv(curr_path).sort("ACCTNO").collect()
    
    if nowk != "4" and REPTMON == "01":
        curr_df = curr_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        crmca_path = BASE_INPUT / "MNICRM" / f"CA{REPTMON1}.csv"
        if crmca_path.exists():
            crmca_df = (
                pl.scan_csv(crmca_path)
                .unique("ACCTNO")
                .drop(["COSTCTR", "FEEPD"])
                .collect()
            )
            curr_df = curr_df.join(crmca_df, on="ACCTNO", how="inner")
    
    if SIGNA_SMSACC.exists():
        saacc_df = pl.scan_csv(SIGNA_SMSACC).sort("ACCTNO").collect()
    else:
        saacc_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    ddwh_df = (
        cis_df.lazy()
        .join(curr_df.lazy(), on="ACCTNO", how="inner")
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
    
    caclose_df = ddwh_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT"),
        pl.col("RISKCODE").cast(pl.Utf8).str.slice(0, 1).alias("RISKRATE")
    ]).drop(["ODATE", "CDATE"])
    
    if PRODEV_DPCLOS.exists():
        dpclos_df = (
            pl.scan_csv(PRODEV_DPCLOS)
            .sort("CLOSEDT", descending=True)
            .unique("ACCTNO", keep="first")
            .select(["ACCTNO", "CLOSEDT", "RCODE"])
            .sort("ACCTNO")
            .collect()
        )
    else:
        dpclos_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "CLOSEDT": pl.Date, "RCODE": pl.Utf8})
    
    if DRCRCA_ACCUM.exists():
        accum_df = pl.scan_csv(DRCRCA_ACCUM).sort("ACCTNO").collect()
    else:
        accum_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    cavg_path = BASE_INPUT / "MISMTD" / f"CAVG{REPTMON}.csv"
    if cavg_path.exists():
        cavg_df = pl.scan_csv(cavg_path).select(["ACCTNO", "MTDAVBAL_MIS"]).collect()
    else:
        cavg_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8, "MTDAVBAL_MIS": pl.Float64})
    
    final_df = (
        caclose_df.lazy()
        .join(dpclos_df.lazy(), on="ACCTNO", how="left")
        .join(accum_df.lazy(), on="ACCTNO", how="left")
        .join(cavg_df.lazy(), on="ACCTNO", how="left")
        .collect()
    )
    
    BASE_OUTPUT.mkdir(parents=True, exist_ok=True)
    output_file = BASE_OUTPUT / f"CACLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result = process_eibmclca()
