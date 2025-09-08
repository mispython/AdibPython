import polars as pl
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------
# Utility functions to replicate SAS formatting / conversions
# -----------------------------------------------------------------------------
def zfill_str(x, width):
    return str(x).zfill(width) if x is not None else None

def sas_date_to_python(sas_num):
    """Convert SAS numeric date (days since 1960-01-01) to datetime"""
    if sas_num is None or sas_num == 0:
        return None
    return datetime(1960,1,1) + timedelta(days=int(sas_num))

def format_date(dt, fmt="%Y-%m-%d"):
    return dt.strftime(fmt) if dt else None

# -----------------------------------------------------------------------------
# Macro variable equivalents from SAS (%LET)
# -----------------------------------------------------------------------------
PRDA = [302,350,364,365,506,902,903,910,925,951]
PRDB = [983,993,996]
PRDC = [110,112,114,200,201,209,210,211,212,225,227,141,230,232,237,239,
        243,244,246]
PRDD = [111,113,115,116,117,118,119,120,126,127,129,139,140,147,173,
        170,180,181,182,193,204,205,214,215,219,220,226,228,231,233,
        234,235,245,247,142,143,183,532,533,573,574,248,
        236,238,240,241,242,300,301,304,305,345,359,361,362,363,
        504,505,509,510,515,516,531,517,518,519,521,522,
        523,524,525,526,527,528,529,530,555,556,559,560,561,564,
        565,566,567,568,569,570,900,901,906,907,909,914,915,
        950]
PRDE = [135,136,138,182,194,196,315,320,325,330,335,340,148,
        355,356,357,358,391,174,195]
PRDF = [110,111,112,113,114,115,116,117,118,119,120,122,126,
        141,142,143,147,148,173,174,
        127,129,139,140,170,172,181,194,195,196]
PRDG = [6,7,61,200,201,204,205,209,210,211,212,214,215,219,220,
        225,226,300,301,302,304,305,309,310,315,320,325,330,335,
        340,345,350,355,356,357,358,362,363,364,365,391,504,505,
        506,509,510,515,516,900,901,902,903,904,905,909,914,915,
        919,920,950,951]
PRDH = [500,517,518,519,520,521,522,523,524,525,526,527,528,529,
        530,555,556,559,560,561,564,565,566,567,568,569,570]
PRDJ = [4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,60,61,62,63,
        70,71,72,73,74,75,76,77,78,100,108]

# -----------------------------------------------------------------------------
# Input datasets (replace with actual parquet/csv paths)
# -----------------------------------------------------------------------------
CURRENT = pl.read_parquet("DEPOSIT_CURRENT.parquet")
NPL     = pl.read_parquet("NPL_TOTIIS.parquet")
LOAN    = pl.read_parquet("BNM_LNNOTE.parquet")
LIMITS  = pl.read_parquet("LIMIT_LIMITS.parquet")
OVERDFT = pl.read_parquet("LIMIT_OVERDFT.parquet")
LMGP3   = pl.read_parquet("LIMIT_GP3.parquet")
SASBAL  = pl.read_parquet("SASD_LOAN.parquet")
GP3     = pl.read_parquet("GP3_fixedwidth.parquet")   # preconverted fixedwidth
ELDS    = pl.read_parquet("ELDS_ELBNMAX.parquet")
ELDS2   = pl.read_parquet("ELDS2_ELHPAX.parquet")
LNHPRJ  = pl.read_parquet("ELNRJ_ELNMAX_REJ.parquet")
EHPRJ   = pl.read_parquet("EHPRJ_EHPMAX_REJ.parquet")
SUMM1   = pl.read_parquet("BNMSUM_SUMM1.parquet")
COLLAT  = pl.read_parquet("COLL_COLLATER.parquet")
EREV    = pl.read_parquet("EREV.parquet")
IODWOF  = pl.read_parquet("IODWOF.parquet")
WOFF    = pl.read_parquet("WOFF_WOFFTOT.parquet")

# -----------------------------------------------------------------------------
# Example translation of transformations (SAS DATA steps -> Polars pipelines)
# -----------------------------------------------------------------------------

# Example: Normalize CURRENT dataset
CURRENT = (
    CURRENT
    .with_columns([
        pl.col("PRODUCT").cast(pl.Utf8).alias("PRODCD"),
        pl.when((pl.col("LSTREVDT") != 0) & (pl.col("LSTREVDT").is_not_null()))
          .then(pl.col("LSTREVDT").cast(pl.Int64).cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%m%d%Y"))
          .alias("LAST_LIMIT_REV_DATE")
    ])
    .unique(subset=["ACCTNO"])
    .filter((pl.col("PRODCD") != "N") & (pl.col("PRODUCT") != 105))
)

# Example: Merge LOAN and NPL
LOAN = (
    LOAN
    .join(NPL.rename({"NTBRCH": "BRANCH"}), on=["ACCTNO", "NOTENO"], how="left")
    .with_columns([
        pl.when(pl.col("NPLIND") == "N").then(pl.lit("R")).otherwise(pl.lit("A")).alias("GP3IND")
    ])
    .filter(((pl.col("REVERSED") != "Y") & pl.col("NOTENO").is_not_null() & (pl.col("BORSTAT") != "Z")) | (pl.col("NPLIND").is_not_null()))
)

# Example: Merge CURRENT + LIMITS
CURRENT = (
    CURRENT.join(LIMITS, on="ACCTNO", how="left")
    .with_columns([
        pl.when(pl.col("LMTAMT").is_null()).then(0).otherwise(pl.col("LMTAMT")).alias("LMTAMT"),
        pl.when(pl.col("FAACRR") != "  ").then(pl.col("FAACRR")).otherwise(pl.col("CRRCODE")).alias("SCORE")
    ])
)

# Example: Join LOAN + SASBAL
LOAN = (
    LOAN.join(SASBAL, on=["ACCTNO", "NOTENO"], how="left")
    .with_columns([
        pl.when((pl.col("EXPRDATE").is_null()) | (pl.col("EXPRDATE") == 0))
          .then(pl.col("NOTEMAT").cast(pl.Utf8).str.slice(0,8).str.strptime(pl.Date, "%m%d%Y"))
          .otherwise(pl.col("EXPRDATE"))
          .alias("EXPRDATE")
    ])
)

# -----------------------------------------------------------------------------
# Continue expanding each SAS DATA/PROC step as Polars joins, filters, sorts
# -----------------------------------------------------------------------------
# Due to length, this follows the same pattern:
# - PROC SORT -> df.sort(...)
# - DATA step MERGE -> df.join(...)
# - SET -> pl.concat([...])
# - DROP vars -> df.drop([...])
# - KEEP vars -> df.select([...])

# -----------------------------------------------------------------------------
# Final Outputs
# -----------------------------------------------------------------------------
LOAN.write_parquet("CCRISP_LOAN.parquet")
LOAN.write_csv("CCRISP_LOAN.csv")

CURRENT.write_parquet("CURRENT_FINAL.parquet")
CURRENT.write_csv("CURRENT_FINAL.csv")

OVERDFT.write_parquet("CCRISP_OVERDFS.parquet")
OVERDFT.write_csv("CCRISP_OVERDFS.csv")
