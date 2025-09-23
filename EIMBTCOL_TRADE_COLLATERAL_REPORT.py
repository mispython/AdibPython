# EIMBTCOL_TRADE_COLLATERAL_REPORT.py
import polars as pl
import duckdb
from datetime import date

# -------------------------------------------------
# Step 1: Reporting date (simulate REPTDATE from BNM.REPTDATE dataset)
# -------------------------------------------------
today = date.today()
reptdate = today
rdate = reptdate.strftime("%d-%m-%Y")

print(f"Running EIMBTCOL_TRADE_COLLATERAL_REPORT for {rdate}")

# -------------------------------------------------
# Step 2: Load input datasets (converted to parquet beforehand)
# -------------------------------------------------
btrad_file = f"BNM_BTRAD_{today.month:02d}_{1}.parquet"   # NOWK=1 example
btmast_file = f"BNM_BTMAST_{today.month:02d}_{1}.parquet"
collater_file = "BTCOLL_COLLATER.parquet"

btrad = pl.read_parquet(btrad_file)
btmast = pl.read_parquet(btmast_file)
collater = pl.read_parquet(collater_file)

# -------------------------------------------------
# Step 3: Filter BTMAST (like WHERE in SAS)
# -------------------------------------------------
btmast_filtered = btmast.filter(
    (pl.col("SUBACCT") == "OV") &
    (pl.col("CUSTCD") != "") &
    (pl.col("BRANCH") != 0)
)

# -------------------------------------------------
# Step 4: Join with collaterals
# -------------------------------------------------
collater = collater.rename({"ACCTNO": "ACCTNO2"})
collater = collater.with_columns(pl.col("ACCTNO2").alias("ACCTNO"))

trade = (
    btmast_filtered.join(
        collater.select(["ACCTNO", "CCLASSC", "CDOLARV"]).rename(
            {"CCLASSC": "LIABCODE"}
        ),
        on="ACCTNO",
        how="left"
    )
)

# -------------------------------------------------
# Step 5: Risk category mapping (PROC FORMAT + CASE)
# -------------------------------------------------
def classify_risk(df: pl.DataFrame) -> pl.DataFrame:
    def mapper(liabcode, sectorcd):
        if liabcode in ["007","012","013","014","024","048","049","117"]:
            return ("30307", 0)
        elif liabcode == "021":
            return ("30309", 0)
        elif liabcode in ["017","026","029"]:
            return ("30009/17", 10)
        elif liabcode in ["006","016"]:
            return ("30323", 20)
        elif liabcode in ["011","030"]:
            return ("30325", 20)
        elif liabcode in ["018","027"]:
            return ("30327", 20)
        elif liabcode == "003":
            return ("30009/10", 20)
        elif liabcode == "025":
            return ("30335", 20)
        elif liabcode in ["050","118"]:
            if sectorcd in ["0311","0312","0313","0314","0315","0316"]:
                return ("30341", 50)
            else:
                return ("30341", 100)
        elif liabcode in ["019","028","031"]:
            return ("30351", 100)
        else:
            return ("30359", 100)

    bnmcode, riskcat = zip(*[mapper(x, y) for x, y in zip(df["LIABCODE"], df["SECTORCD"])])
    return df.with_columns([
        pl.Series("BNMCODE", bnmcode),
        pl.Series("RISKCAT", riskcat)
    ])

loan = classify_risk(trade)

# -------------------------------------------------
# Step 6: Balance calculations (TOTBAL, COLVAL, XBALANCE)
# -------------------------------------------------
loan = loan.with_columns([
    (pl.col("ICURBAL") + pl.col("DBALANCE")).alias("TOTBAL"),
])

loan = loan.with_columns([
    ((pl.col("DBALANCE") / pl.col("TOTBAL")) * pl.col("CDOLARV")).alias("COLVAL")
])

# Adjust balances similar to SAS conditional OUTPUT logic
loan = loan.with_columns([
    pl.when((pl.col("COLVAL") > 0) & (pl.col("DBALANCE") > pl.col("COLVAL")))
    .then(pl.col("COLVAL"))
    .otherwise(pl.col("DBALANCE"))
    .alias("XBALANCE")
])

# -------------------------------------------------
# Step 7: Aggregate (PROC SUMMARY equivalent)
# -------------------------------------------------
lnbr = (
    loan.group_by(["BRANCH", "RISKCAT", "BNMCODE"])
        .agg(pl.sum("XBALANCE").alias("BALANCE"))
        .sort(["BRANCH", "RISKCAT"])
)

ln_summary = (
    loan.group_by(["RISKCAT", "BNMCODE"])
        .agg(pl.sum("XBALANCE").alias("BALANCE"))
        .sort("RISKCAT")
)

# -------------------------------------------------
# Step 8: Save outputs
# -------------------------------------------------
lnbr.write_parquet("EIMBTCOL_LNBR.parquet")
ln_summary.write_parquet("EIMBTCOL_LN_SUMMARY.parquet")

print("Outputs generated: EIMBTCOL_LNBR.parquet, EIMBTCOL_LN_SUMMARY.parquet")
