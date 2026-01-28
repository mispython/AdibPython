import polars as pl
from datetime import datetime
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
BNM_PATH = BASE_PATH / "bnm"
MNITR_PATH = BASE_PATH / "mnitr"
CISSAFD_PATH = BASE_PATH / "cissafd"
CISCA_PATH = BASE_PATH / "cisca"
TRBL_PATH = BASE_PATH / "trbl"
OUTPUT_PATH = BASE_PATH / "output"

# ==================== REPTDATE PROCESSING ====================
reptdate_df = pl.read_parquet(BNM_PATH / "REPTDATE.parquet")
reptdate = reptdate_df[0, "REPTDATE"]

mmd = reptdate.month
if mmd in (1, 2, 3): qtr = 'Q1'
elif mmd in (4, 5, 6): qtr = 'Q2'
elif mmd in (7, 8, 9): qtr = 'Q3'
else: qtr = 'Q4'

typ = 'A'
day_val = reptdate.day
if day_val == 8: wk = '1'
elif day_val == 15: wk = '2'
elif day_val == 22: wk = '3'
else: wk = '4'

NOWK = wk
YYD = str(reptdate.year)[-2:]
QTR = qtr
TYP = typ
MMD = f"{mmd:02d}"
XDATE = reptdate.strftime("%d%m%Y")

print(f"Processing: Month={MMD}, Quarter={QTR}, Week={NOWK}, Year={YYD}")

# ==================== CIS CUSTOMER DATA ====================
ciscust = pl.concat([
    pl.read_parquet(CISSAFD_PATH / "DEPOSIT.parquet"),
    pl.read_parquet(CISCA_PATH / "DEPOSIT.parquet")
]).filter(pl.col("SECCUST") == "901").select([
    "ACCTNO", "CITIZEN", pl.col("CUSTNAM1").alias("NAME")
]).unique(subset=["ACCTNO"])

# ==================== PROCESS RMCA & RMSA ====================
def process_rm_data(file_prefix, custcode_range, product_filter=None):
    df = pl.read_parquet(MNITR_PATH / f"{file_prefix}{QTR}{YYD}.parquet")
    df = df.filter(
        (pl.col("CUSTCODE").is_between(80, 99)) &
        (pl.col("TRXDATE").str.slice(3, 2) == MMD)
    )
    if product_filter:
        df = df.filter(product_filter)
    return df.with_columns(pl.lit(XDATE).str.strptime(pl.Date, "%d%m%Y").alias("TRXNDATE"))

# Process RMCA
rmca = process_rm_data("RMCA", (80, 99))

# Process RMSA
product_conditions = (
    (pl.col("PRODUCT").is_between(200, 208)) |
    (pl.col("PRODUCT").is_between(212, 216)) |
    (pl.col("PRODUCT").is_in([210, 218, 220, 221, 222, 223]))
)
rmsa = process_rm_data("RMSA", (80, 99), product_conditions)

# Combine and summarize
rmdd = pl.concat([rmca, rmsa])
rmdd_sum = rmdd.group_by("ACCTNO").agg([
    pl.col("CREDITFY").sum(),
    pl.col("DEBITFY").sum(),
    pl.col("CREDITMY").sum(),
    pl.col("DEBITMY").sum()
])
rmdd = rmdd.unique(subset=["ACCTNO"]).join(rmdd_sum, on="ACCTNO", how="inner")

# ==================== PROCESS CA & SA DATA ====================
def load_account_data(file_type, is_islamic=False):
    prefix = "BNMI" if is_islamic else "BNM"
    return pl.read_parquet(BNM_PATH / f"{prefix}.{file_type}.parquet")

# Current Accounts
ca_conv = load_account_data("CURRENT", False).with_columns(pl.col("CURCODE").alias("CURCD"))
ca_islamic = load_account_data("CURRENT", True).with_columns(pl.col("CURCODE").alias("CURCD"))
ca_all = pl.concat([ca_conv, ca_islamic])

# Filter CA products
ca_products = [30,31,32,33,34,81,82,83,84,100,101,102,103,108,109,110,111,112,113,
               114,115,116,117,118,119,120,121,122,123,124,125,150,151,152,153,154,
               155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,174,175,
               176,177,178,179,180,181,96,97,189,190,191,192,193,194,195,196,197,
               40,41,42,43,72,73,74,75,76,106,133,134,170,182,20,21,92,23,24,25]

ca = ca_all.filter(
    (pl.col("CUSTCODE").is_between(80, 99)) &
    (pl.col("PRODUCT").is_in(ca_products))
)

# Savings Accounts
sa_conv = load_account_data("SAVING", False).with_columns(pl.col("CURCODE").alias("CURCD"))
sa_islamic = load_account_data("SAVING", True).with_columns(pl.col("CURCODE").alias("CURCD"))
sa_all = pl.concat([sa_conv, sa_islamic])

sa = sa_all.filter(
    (pl.col("CUSTCODE").is_between(80, 99)) &
    (
        (pl.col("PRODUCT").is_between(200, 208)) |
        (pl.col("PRODUCT").is_between(212, 216)) |
        (pl.col("PRODUCT").is_in([210, 218, 220, 221, 222, 223]))
    )
)

rmdd1 = pl.concat([ca, sa])

# ==================== COST CENTER DATA ====================
costctr_cols = ["ACCTNO", "COSTCTR"]
cacctr = ca.select(costctr_cols)
sacctr = sa.select(costctr_cols)

uma_conv = load_account_data("UMA", False).select(costctr_cols)
uma_islamic = load_account_data("UMA", True).select(costctr_cols)
umacctr = pl.concat([uma_conv, uma_islamic])

cctr = pl.concat([cacctr, sacctr, umacctr]).unique(subset=["ACCTNO"])

# ==================== FINAL RMDD PROCESSING ====================
rmdd_final = rmdd.join(rmdd1, on="ACCTNO", how="left", suffix="_right").with_columns([
    pl.when(pl.col("CURBAL") <= 0).then(0).otherwise(pl.col("CURBAL")).alias("CURBAL"),
    pl.col("CREDITFY").fill_null(0),
    pl.col("DEBITFY").fill_null(0),
    pl.col("CREDITMY").fill_null(0),
    pl.col("DEBITMY").fill_null(0),
    pl.col("DEBITMY").alias("DEBITMYR"),
    pl.col("DEBITFY").alias("DEBITFCY"),
    pl.col("CREDITMY").alias("CREDITMYR"),
    pl.col("CREDITFY").alias("CREDITFCY")
])

# Merge with CIS customer and cost center data
rmdd_output = rmdd_final.join(ciscust, on="ACCTNO", how="left").join(cctr, on="ACCTNO", how="left")

# Select required columns
depo_cols = ["BRANCH", "ACCTNO", "CUSTCODE", "PRODUCT", "CURCD", "CITIZEN",
             "TRXNDATE", "CREDITFY", "DEBITFY", "CREDITMY", "DEBITMY",
             "NAME", "COSTCTR", "CREDITFCY", "DEBITFCY", "CREDITMYR", "DEBITMYR"]

rmdd_output = rmdd_output.select([col for col in depo_cols if col in rmdd_output.columns])

# Save output
output_file = TRBL_PATH / f"RMDDSAACE{MMD}{NOWK}{YYD}{TYP}.parquet"
rmdd_output.write_parquet(output_file)
print(f"Saved RMDD: {output_file}")

# ==================== PROCESS FXDD ====================
fxdd = process_rm_data("FXCA", (80, 99))
fxdd_sum = fxdd.group_by("ACCTNO").agg([
    pl.col("CREDITFY").sum(),
    pl.col("DEBITFY").sum(),
    pl.col("CREDITMY").sum(),
    pl.col("DEBITMY").sum()
])
fxdd = fxdd.unique(subset=["ACCTNO"]).join(fxdd_sum, on="ACCTNO", how="inner")

# Filter FX accounts
fx_products = list(range(400, 412)) + list(range(440, 445)) + [413] + \
              list(range(450, 455)) + list(range(420, 435))

fx = ca_all.filter(
    (pl.col("CUSTCODE").is_between(80, 99)) &
    (pl.col("PRODUCT").is_in(fx_products))
).select(["ACCTNO", "NAME", "BRANCH", "CUSTCODE", "PRODUCT", 
          "CURCD", "CURBAL", "FORBAL", "COSTCTR"])

fxdd_final = fxdd.join(fx, on="ACCTNO", how="left").with_columns([
    pl.when(pl.col("CURBAL") <= 0).then(0).otherwise(pl.col("CURBAL")).alias("CURBAL"),
    pl.col("FORBAL").fill_null(0),
    pl.col("CREDITFY").fill_null(0),
    pl.col("DEBITFY").fill_null(0),
    pl.col("CREDITMY").fill_null(0),
    pl.col("DEBITMY").fill_null(0),
    pl.col("DEBITMY").alias("DEBITMYR"),
    pl.col("DEBITFY").alias("DEBITFCY"),
    pl.col("CREDITMY").alias("CREDITMYR"),
    pl.col("CREDITFY").alias("CREDITFCY")
])

fxdd_output = fxdd_final.join(ciscust, on="ACCTNO", how="left").join(cctr, on="ACCTNO", how="left")
fxdd_output = fxdd_output.select([col for col in depo_cols if col in fxdd_output.columns])

fxdd_file = TRBL_PATH / f"FXDD{MMD}{NOWK}{YYD}{TYP}.parquet"
fxdd_output.write_parquet(fxdd_file)
print(f"Saved FXDD: {fxdd_file}")

# ==================== PROCESS VOSTRO ====================
vost = pl.read_parquet(MNITR_PATH / f"VOST{QTR}{YYD}.parquet").filter(
    pl.col("TRXDATE").str.slice(3, 2) == MMD
).with_columns(pl.lit(XDATE).str.strptime(pl.Date, "%d%m%Y").alias("TRXNDATE"))

vost_sum = vost.group_by("ACCTNO").agg([
    pl.col("CREDITFY").sum(),
    pl.col("DEBITFY").sum(),
    pl.col("CREDITMY").sum(),
    pl.col("DEBITMY").sum()
])
vost = vost.unique(subset=["ACCTNO"]).join(vost_sum, on="ACCTNO", how="inner")

vost1 = ca_all.filter(pl.col("PRODUCT") == 105)
vost_final = vost1.join(vost, on="ACCTNO", how="inner").with_columns([
    pl.when(pl.col("CURBAL") <= 0).then(0).otherwise(pl.col("CURBAL")).alias("CURBAL"),
    pl.col("CREDITFY").fill_null(0),
    pl.col("DEBITFY").fill_null(0),
    pl.col("CREDITMY").fill_null(0),
    pl.col("DEBITMY").fill_null(0),
    pl.col("DEBITMY").alias("DEBITMYR"),
    pl.col("DEBITFY").alias("DEBITFCY"),
    pl.col("CREDITMY").alias("CREDITMYR"),
    pl.col("CREDITFY").alias("CREDITFCY")
])

vost_output = vost_final.join(ciscust, on="ACCTNO", how="left").join(cctr, on="ACCTNO", how="left")
vost_output = vost_output.select([col for col in depo_cols if col in vost_output.columns])

vost_file = TRBL_PATH / f"VOSTRO{MMD}{NOWK}{YYD}{TYP}.parquet"
vost_output.write_parquet(vost_file)
print(f"Saved VOSTRO: {vost_file}")

# ==================== SUMMARY ====================
print(f"\nProcessing Complete!")
print(f"RMDD Records: {len(rmdd_output)}")
print(f"FXDD Records: {len(fxdd_output)}")
print(f"VOSTRO Records: {len(vost_output)}")
print(f"Output files saved to: {TRBL_PATH}")
