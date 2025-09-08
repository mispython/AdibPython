import polars as pl
from datetime import datetime

# -----------------------------
# Utility functions
# -----------------------------
def zfill_str(val, length):
    return str(val).zfill(length) if val is not None else None

def sas_to_date(num):
    """Convert SAS numeric date (yyyymmddnnn) into datetime."""
    try:
        s = str(int(num)).zfill(11)
        return datetime.strptime(s[:8], "%m%d%Y")
    except:
        return None

# -----------------------------
# Load raw datasets (replace with actual file paths or DB extractions)
# -----------------------------
bnm_reptdate = pl.read_csv("BNM_REPTDATE.csv")
deposit_current = pl.read_csv("DEPOSIT_CURRENT.csv")
npl_totiis = pl.read_csv("NPL_TOTIIS.csv")
bnm_lnnote = pl.read_csv("BNM_LNNOTE.csv")
bnmo_lnnote = pl.read_csv("BNMO_LNNOTE.csv")
limit_overdft = pl.read_csv("LIMIT_OVERDFT.csv")
limit_gp3 = pl.read_csv("LIMIT_GP3.csv")
limit_limits = pl.read_csv("LIMIT_LIMITS.csv")
sasd_loan = pl.read_csv("SASD_LOAN.csv")
gp3_raw = pl.read_csv("GP3.csv")
elds1 = pl.read_csv("ELDS_ELAA.csv")
elds2 = pl.read_csv("ELDS_ELHP.csv")
elnrj = pl.read_csv("ELNRJ.csv")
ehprj = pl.read_csv("EHPRJ.csv")
bnmsum = pl.read_csv("BNMSUM.csv")
coll_collater = pl.read_csv("COLL_COLLATER.csv")
ccrisx = pl.read_csv("CCRISX.csv")

# -----------------------------
# Step 1: Dates logic (DATA DATES)
# -----------------------------
dates = (
    bnm_reptdate
    .with_columns([
        pl.col("REPTDATE").dt.day().alias("DAYS"),
        pl.col("REPTDATE").dt.month().alias("MONTHS"),
        pl.col("REPTDATE").dt.year().alias("YEARS"),
    ])
)

# Derive week indicators (WK, WK1, etc.)
dates = dates.with_columns([
    pl.when(pl.col("DAYS") == 8).then(pl.lit(1))
     .when(pl.col("DAYS") == 15).then(pl.lit(9))
     .when(pl.col("DAYS") == 22).then(pl.lit(16))
     .otherwise(23).alias("SDD"),

    pl.when(pl.col("DAYS") == 8).then(pl.lit("1"))
     .when(pl.col("DAYS") == 15).then(pl.lit("2"))
     .when(pl.col("DAYS") == 22).then(pl.lit("3"))
     .otherwise("4").alias("WK")
])

# -----------------------------
# Step 2: Deposit CURRENT (with PRODCD mapping and limit rev date)
# -----------------------------
current = (
    deposit_current
    .with_columns([
        # Example: PRODCD mapping must come from external mapping (CAPROD.)
        pl.col("PRODUCT").cast(pl.Utf8).alias("PRODCD"),

        pl.when((pl.col("LSTREVDT").is_not_null()) & (pl.col("LSTREVDT") != 0))
          .then(pl.col("LSTREVDT").map_elements(sas_to_date))
          .otherwise(None).alias("LAST_LIMIT_REV_DATE")
    ])
)

# -----------------------------
# Step 3: Sort / Deduplicate
# -----------------------------
current = current.unique(subset=["ACCTNO"])
npl = npl_totiis.unique(subset=["ACCTNO", "NOTENO"])
loan = bnm_lnnote.rename({"PENDBRH": "BRANCH"}).unique(subset=["ACCTNO", "NOTENO"])
loano = bnmo_lnnote.rename({"PENDBRH": "OLDBRH"}).select(["ACCTNO", "NOTENO", "OLDBRH"])

# -----------------------------
# Step 4: Merge LOAN with OLDNOTE + LOANO
# -----------------------------
loan = (
    loan.join(loano, on=["ACCTNO", "NOTENO"], how="left")
    .with_columns([
        pl.col("OLDBRH").fill_null(0)
    ])
)

# -----------------------------
# Step 5: Merge with NPL
# -----------------------------
loan = (
    loan.join(
        npl.rename({"NTBRCH": "BRANCH"}), 
        on=["ACCTNO", "NOTENO"], how="outer"
    )
    .with_columns([
        pl.when(pl.col("NPLIND") == "N").then("R").otherwise("A").alias("GP3IND")
    ])
)

# -----------------------------
# Step 6: LIMIT + CURRENT
# -----------------------------
limits = limit_limits
current = current.join(limits, on="ACCTNO", how="left")
current = current.with_columns([
    pl.col("LMTAMT").fill_null(0),
    pl.when(pl.col("FAACRR") != "  ").then(pl.col("FAACRR"))
     .otherwise(pl.col("CRRCODE")).alias("SCORE")
])

# -----------------------------
# Step 7: SASBAL join to LOAN
# -----------------------------
sasbal = sasd_loan.filter(~pl.col("PRODUCT").is_in([983,993,996]))
loan = loan.join(sasbal, on=["ACCTNO", "NOTENO"], how="left")

# -----------------------------
# Step 8: GP3 file read (fixed-width equivalent already as CSV here)
# -----------------------------
gp3 = gp3_raw.rename({"NOTENO": "NOTENO"}).with_columns([
    pl.lit(None).alias("NOTENO")
])

# -----------------------------
# Step 9: Merge CURRENT with OVERDFT, GP3, LMGP3, CCPT
# -----------------------------
overdft = limit_overdft.filter(~pl.col("PRODUCT").is_in([105,107,160]))
lm_gp3 = limit_gp3
# CCPT calculation: simplified
ccpt = (
    limit_overdft
    .with_columns([
        (pl.col("LAST_LIMIT_REV_DATE") - pl.col("LMTSTDTE")).alias("CCPT_DAYS")
    ])
    .with_columns([
        pl.when(pl.col("CCPT_DAYS") > 0).then(0).otherwise(1).alias("CCPT_SEQ")
    ])
)

current = current.join(overdft, on="ACCTNO", how="left")
current = current.join(gp3, on="ACCTNO", how="left")
current = current.join(lm_gp3, on="ACCTNO", how="left")
current = current.join(ccpt, on="ACCTNO", how="left")

# -----------------------------
# Step 10: Merge LOAN with COMM
# -----------------------------
comm = pl.read_csv("BNM_LNCOMM.csv").unique(subset=["ACCTNO", "COMMNO"])
loan = loan.join(comm, on=["ACCTNO", "COMMNO"], how="left")

# -----------------------------
# Step 11: Merge with ELDS (ELAA + ELHP + Rejects)
# -----------------------------
elds = pl.concat([elds1, elds2], how="diagonal")
lnhprj = pl.concat([elnrj, ehprj], how="diagonal").unique(subset=["AANO"])
elds = elds.join(lnhprj, on="AANO", how="left")

# -----------------------------
# Step 12: Merge LOAN with ELDS + SUMM1
# -----------------------------
summ1 = bnmsum.unique(subset=["AANO"])
loan = loan.join(elds, on="AANO", how="left")
loan = loan.join(summ1, on="AANO", how="left")

# -----------------------------
# Step 13: Collateral merge
# -----------------------------
collat = coll_collater.unique(subset=["ACCTNO", "NOTENO"])
loan = loan.join(collat, on=["ACCTNO", "NOTENO"], how="left")

# -----------------------------
# Step 14: CCRISX exclusion
# -----------------------------
loan = loan.join(ccrisx, on=["ACCTNO", "NOTENO"], how="left")
loan = loan.filter(pl.col("CCRISX_field").is_null())  # exclude those present

# -----------------------------
# Final outputs
# -----------------------------
loan.write_parquet("loan_final.parquet")
current.write_parquet("current_final.parquet")
overdft.write_parquet("overdft_final.parquet")
elds.write_parquet("elds_final.parquet")
