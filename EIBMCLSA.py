import polars as pl
from pathlib import Path
import datetime

BASE_INPUT = Path("C:/Your/Base/Input/Path")
BASE_OUTPUT = Path("C:/Your/Base/Output/Path")

DEP_REPTDATE = BASE_INPUT / "DEP" / "REPTDATE.csv"
CIS_DEPOSIT = BASE_INPUT / "CIS" / "DEPOSIT.csv"
SIGNA_SMSACC = BASE_INPUT / "SIGNA" / "SMSACC.csv"
PRODEV_DPCLOS = BASE_INPUT / "PRODEV" / "DPCLOS.csv"

def parse_sas_date(date_col):
    return (
        date_col.cast(pl.Utf8)
        .str.zfill(11)
        .str.slice(0, 8)
        .str.strptime(pl.Date, format="%m%d%Y")
    )

def process_eibmclsa():
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
    
    savg_path = BASE_INPUT / "MIS" / f"SAVGC{REPTMON}.csv"
    if not savg_path.exists():
        raise FileNotFoundError(f"SAVG file not found: {savg_path}")
    
    savg_df = pl.scan_csv(savg_path).sort("ACCTNO").collect()
    
    if REPTMON != REPTMON1 and REPTMON == "01":
        savg_df = savg_df.with_columns(pl.lit(0).alias("FEEYTD"))
    else:
        crmsa_path = BASE_INPUT / "MNICRM" / f"SA{REPTMON1}.csv"
        if crmsa_path.exists():
            crmsa_df = (
                pl.scan_csv(crmsa_path)
                .unique("ACCTNO")
                .drop("COSTCTR")
                .collect()
            )
            savg_df = savg_df.join(crmsa_df, on="ACCTNO", how="inner")
    
    if SIGNA_SMSACC.exists():
        saacc_df = pl.scan_csv(SIGNA_SMSACC).sort("ACCTNO").collect()
    else:
        saacc_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    ddwh_df = (
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
    
    saclose_df = ddwh_df.with_columns([
        pl.col("ODATE").alias("OPENDT"),
        pl.col("CDATE").alias("CLOSEDT")
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
    
    savg_mtd_path = BASE_INPUT / "MISMTD" / f"SAVG{REPTMON}.csv"
    if savg_mtd_path.exists():
        savg_mtd_df = pl.scan_csv(savg_mtd_path).collect()
    else:
        savg_mtd_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    
    final_df = (
        saclose_df.lazy()
        .join(dpclos_df.lazy(), on="ACCTNO", how="left")
        .join(savg_mtd_df.lazy(), on="ACCTNO", how="left")
        .collect()
    )
    
    BASE_OUTPUT.mkdir(parents=True, exist_ok=True)
    output_file = BASE_OUTPUT / f"SACLOSE{REPTMON}{REPTYEAR}.parquet"
    final_df.write_parquet(output_file, compression="zstd", statistics=True)
    
    return final_df

if __name__ == "__main__":
    result = process_eibmclsa()
