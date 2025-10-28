# eiwibt1c_collateral_maturity_report.py
import duckdb
import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime
import sys

# Simulate %INC PGM(PBBLNFMT) - Assume this loads formats
# PROC FORMAT equivalent
remfmts_format = {
    "range": ["<1 YEAR", ">1 YEAR"],
    "breaks": [12]
}

remfmt_format = {
    "range": ["UP TO 1 WK", ">1 WK - 1 MTH", ">1 MTH - 3 MTHS", ">3 - 6 MTHS", ">6 MTHS - 1 YR", ">1 YEAR"],
    "breaks": [0.255, 1, 3, 6, 12]
}

typfmt_format = {
    1: "PBB OWN FIXED DEPOSIT, 30308 (0%)",
    2: "DISCOUNT HOUSE/CAGAMAS, 30314 (10%)", 
    3: "FIN INST FIXED DEPOSIT/GUARANTEE, 30332 (20%)",
    4: "STATUTORY BODIES, 30336 (20%)",
    5: "FIRST CHARGE, 30342 (50%)",
    6: "SHARES/UNIT TRUSTS, 30360 (100%)",
    7: "OTHERS, 30360 (100%)"
}

# DATA _NULL_ equivalent for date parameters
reptdate_df = duckdb.connect().execute("SELECT * FROM read_parquet('BNM/REPTDATE.parquet')").pl()
reptdate = reptdate_df["REPTDATE"][0]

day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2' 
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
reptyear = reptdate.strftime("%y")
rdate = reptdate.strftime("%d%m%y")

# DATA BTRADI - Read IBTDTL data
btradi_df = duckdb.connect().execute(
    f"SELECT * FROM read_parquet('BNMDAILY/IBTDTL{reptyear}{reptmon}{reptday}.parquet')"
).pl().filter(pl.col("LIABCODE") != "")

# Apply BTPROD formats (simulated)
btradi_df = btradi_df.with_columns([
    pl.when(pl.col("DIRCTIND") == "D").then(pl.col("LIABCODE"))
     .when(pl.col("DIRCTIND") == "I").then(pl.col("LIABCODE") + "I")
     .otherwise(pl.col("LIABCODE")).alias("PRODCD")
])

# ORIGMT calculation
btradi_df = btradi_df.with_columns([
    pl.when((pl.col("EXPRDATE") - pl.col("ISSDTE")).dt.total_days() < 366)
     .then(pl.lit("10")).otherwise(pl.lit("20")).alias("ORIGMT")
])

btradi_df = btradi_df.sort(["BRANCH", "ACCTNO", "TRANSREF"])

# PROC SUMMARY equivalent
btradia_df = btradi_df.group_by(["BRANCH", "ACCTNO", "PRODCD"]).agg([
    pl.col("OUTSTAND").sum().alias("OUTSTAND")
])

btradia_df = btradia_df.sort("ACCTNO")

# Read IBTMAST data
btradm_df = duckdb.connect().execute(
    f"SELECT * FROM read_parquet('BNM/IBTMAST{reptmon}{nowk}.parquet')"
).pl().filter(pl.col("SUBACCT") == "OV").unique(subset=["ACCTNO"])

# DATA BTRADE - Merge
btrade_df = btradm_df.join(btradia_df, on="ACCTNO", how="inner")

# DATA LOAN - Apply %COLL macro
loan_df = btrade_df.with_columns([
    pl.when(pl.col("LIABCODE").is_in(['007','012','013','014','021','024','048','049'])).then(1)
    .when(pl.col("LIABCODE").is_in(['017','026','029'])).then(2)
    .when(pl.col("LIABCODE").is_in(['006','011','016','030','018','027','003'])).then(3)
    .when(pl.col("LIABCODE") == '025').then(4)
    .when(pl.col("LIABCODE") == '050').then(5)
    .when(pl.col("LIABCODE").is_in(['015','008','042'])).then(6)
    .otherwise(7).alias("TYP")
]).with_columns([
    pl.col("PRODCD").alias("BNMCODE")
]).filter(pl.col("BNMCODE") != "").sort("BRANCH")

# DATA LOAN2 - Apply %DCLVAR and %REMMTH macros
loan2_df = btradi_df.sort(["ACCTNO", "TRANSREF"]).with_columns([
    pl.lit(reptdate.year).alias("RPYR"),
    pl.lit(reptdate.month).alias("RPMTH"), 
    pl.lit(reptdate.day).alias("RPDAY")
])

# Apply %DCLVAR macro
loan2_df = loan2_df.with_columns([
    pl.lit(31).alias("D1"), pl.lit(31).alias("D2"), pl.lit(31).alias("D3"),
    pl.lit(30).alias("D4"), pl.lit(31).alias("D5"), pl.lit(30).alias("D6"),
    pl.lit(31).alias("D7"), pl.lit(31).alias("D8"), pl.lit(30).alias("D9"),
    pl.lit(31).alias("D10"), pl.lit(30).alias("D11"), pl.lit(31).alias("D12"),
    pl.lit(31).alias("RD1"), pl.lit(28).alias("RD2"), pl.lit(31).alias("RD3"),
    pl.lit(30).alias("RD4"), pl.lit(31).alias("RD5"), pl.lit(30).alias("RD6"),
    pl.lit(31).alias("RD7"), pl.lit(31).alias("RD8"), pl.lit(30).alias("RD9"),
    pl.lit(31).alias("RD10"), pl.lit(30).alias("RD11"), pl.lit(31).alias("RD12"),
    pl.lit(31).alias("MD1"), pl.lit(28).alias("MD2"), pl.lit(31).alias("MD3"),
    pl.lit(30).alias("MD4"), pl.lit(31).alias("MD5"), pl.lit(30).alias("MD6"),
    pl.lit(31).alias("MD7"), pl.lit(31).alias("MD8"), pl.lit(30).alias("MD9"),
    pl.lit(31).alias("MD10"), pl.lit(30).alias("MD11"), pl.lit(31).alias("MD12")
])

loan2_df = loan2_df.with_columns([
    pl.when(pl.col("ORIGMT") < "20").then(1.0).otherwise(13.0).alias("REMMTH"),
    pl.col("PRODCD").alias("BNMCODE"),
    pl.col("EXPRDATE").alias("MATDT")
])

# Apply %REMMTH macro
loan2_df = loan2_df.with_columns([
    pl.col("MATDT").dt.year().alias("MDYR"),
    pl.col("MATDT").dt.month().alias("MDMTH"), 
    pl.col("MATDT").dt.day().alias("MDDAY"),
    pl.when(pl.col("MDMTH") == 2)
     .then(pl.when(pl.col("MDYR") % 4 == 0).then(29).otherwise(28))
     .otherwise(pl.col("MDDAY")).alias("MDDAY_ADJ"),
    (pl.col("MDYR") - pl.col("RPYR")).alias("REMY"),
    (pl.col("MDMTH") - pl.col("RPMTH")).alias("REMM"),
    (pl.col("MDDAY_ADJ") - pl.col("RPDAY")).alias("REMD")
]).with_columns([
    (pl.col("REMY") * 12 + pl.col("REMM") + pl.col("REMD") / 
     pl.when(pl.col("RPMTH") == 1).then(pl.col("RD1"))
     .when(pl.col("RPMTH") == 2).then(pl.col("RD2"))
     .when(pl.col("RPMTH") == 3).then(pl.col("RD3"))
     .when(pl.col("RPMTH") == 4).then(pl.col("RD4"))
     .when(pl.col("RPMTH") == 5).then(pl.col("RD5"))
     .when(pl.col("RPMTH") == 6).then(pl.col("RD6"))
     .when(pl.col("RPMTH") == 7).then(pl.col("RD7"))
     .when(pl.col("RPMTH") == 8).then(pl.col("RD8"))
     .when(pl.col("RPMTH") == 9).then(pl.col("RD9"))
     .when(pl.col("RPMTH") == 10).then(pl.col("RD10"))
     .when(pl.col("RPMTH") == 11).then(pl.col("RD11"))
     .when(pl.col("RPMTH") == 12).then(pl.col("RD12"))
     .otherwise(30)).alias("REMMTH")
]).filter(pl.col("BNMCODE") != "").select([
    "BRANCH", "BNMCODE", "REMMTH", "OUTSTAND", "LIABCODE", 
    "ISSDTE", "EXPRDATE", "ACCTNO"
])

# Apply TYP for LOAN2 (needed for reports)
loan2_df = loan2_df.with_columns([
    pl.when(pl.col("LIABCODE").is_in(['007','012','013','014','021','024','048','049'])).then(1)
    .when(pl.col("LIABCODE").is_in(['017','026','029'])).then(2)
    .when(pl.col("LIABCODE").is_in(['006','011','016','030','018','027','003'])).then(3)
    .when(pl.col("LIABCODE") == '025').then(4)
    .when(pl.col("LIABCODE") == '050').then(5)
    .when(pl.col("LIABCODE").is_in(['015','008','042'])).then(6)
    .otherwise(7).alias("TYP")
])

loan2_df = loan2_df.sort("BRANCH")

# Apply formats using direct mapping
loan_df = loan_df.with_columns([
    pl.col("TYP").map_elements(lambda x: typfmt_format.get(x, "UNKNOWN"), return_dtype=pl.Utf8).alias("TYP_FMT")
])

loan2_df = loan2_df.with_columns([
    pl.col("REMMTH").map_elements(lambda x: "UP TO 1 WK" if x <= 0.255 
                                 else ">1 WK - 1 MTH" if x <= 1 
                                 else ">1 MTH - 3 MTHS" if x <= 3 
                                 else ">3 - 6 MTHS" if x <= 6 
                                 else ">6 MTHS - 1 YR" if x <= 12 
                                 else ">1 YEAR", return_dtype=pl.Utf8).alias("REMMTH_FMT"),
    pl.col("REMMTH").map_elements(lambda x: "<1 YEAR" if x <= 12 else ">1 YEAR", return_dtype=pl.Utf8).alias("REMMTH_FMTS"),
    pl.col("TYP").map_elements(lambda x: typfmt_format.get(x, "UNKNOWN"), return_dtype=pl.Utf8).alias("TYP_FMT")
])

# PROC PRINTTO equivalent - redirect output
output_file = Path(f"EIWIBT1C_{reptyear}{reptmon}{reptday}.txt")
original_stdout = sys.stdout

try:
    with open(output_file, 'w') as f:
        sys.stdout = f
        
        # TITLE statements
        print("REPORT ID: EIWIBT1C")
        print(f"PUBLIC BANK BERHAD            DATE : {rdate}")
        print()
        
        # First PROC TABULATE - Collateral Report
        print("REPORT ON COLLATERAL")
        collateral_report = loan_df.group_by(["BRANCH", "BNMCODE", "TYP_FMT"]).agg([
            pl.col("OUTSTAND").sum().alias("TOTAL_OUTSTAND")
        ])
        
        # Print branch-level totals
        for branch in collateral_report["BRANCH"].unique():
            branch_data = collateral_report.filter(pl.col("BRANCH") == branch)
            print(f"BRANCH: {branch}")
            for typ in branch_data["TYP_FMT"].unique():
                typ_data = branch_data.filter(pl.col("TYP_FMT") == typ)
                typ_total = typ_data["TOTAL_OUTSTAND"].sum()
                print(f"  {typ}: {typ_total:13.2f}")
            branch_total = branch_data["TOTAL_OUTSTAND"].sum()
            print(f"  BRANCH TOTAL: {branch_total:13.2f}")
            print()
        
        grand_total = collateral_report["TOTAL_OUTSTAND"].sum()
        print(f"GRAND TOTAL: {grand_total:13.2f}")
        print("\n" + "="*80 + "\n")
        
        # Second PROC TABULATE - Remaining Maturity (detailed)
        print("REPORT ON REMAINING MATURITY")
        maturity_report = loan2_df.group_by(["BNMCODE", "REMMTH_FMT"]).agg([
            pl.col("OUTSTAND").sum().alias("TOTAL_OUTSTAND")
        ])
        
        for bnmcode in maturity_report["BNMCODE"].unique():
            code_data = maturity_report.filter(pl.col("BNMCODE") == bnmcode)
            print(f"BNMCODE: {bnmcode}")
            for rem_cat in remfmt_format["range"]:
                cat_data = code_data.filter(pl.col("REMMTH_FMT") == rem_cat)
                cat_total = cat_data["TOTAL_OUTSTAND"].sum() if len(cat_data) > 0 else 0.0
                print(f"  {rem_cat}: {cat_total:13.2f}")
            code_total = code_data["TOTAL_OUTSTAND"].sum()
            print(f"  CODE TOTAL: {code_total:13.2f}")
            print()
        
        print("\n" + "="*80 + "\n")
        
        # Third PROC TABULATE - Remaining Maturity (simplified)
        print("REPORT ON REMAINING MATURITY")
        maturity_simple = loan2_df.group_by(["BNMCODE", "REMMTH_FMTS"]).agg([
            pl.col("OUTSTAND").sum().alias("TOTAL_OUTSTAND")
        ])
        
        for bnmcode in maturity_simple["BNMCODE"].unique():
            code_data = maturity_simple.filter(pl.col("BNMCODE") == bnmcode)
            print(f"BNMCODE: {bnmcode}")
            for rem_cat in remfmts_format["range"]:
                cat_data = code_data.filter(pl.col("REMMTH_FMTS") == rem_cat)
                cat_total = cat_data["TOTAL_OUTSTAND"].sum() if len(cat_data) > 0 else 0.0
                print(f"  {rem_cat}: {cat_total:13.2f}")
            code_total = code_data["TOTAL_OUTSTAND"].sum()
            print(f"  CODE TOTAL: {code_total:13.2f}")
            print()

finally:
    sys.stdout = original_stdout

print(f"Report generated: {output_file}")

# Write output datasets
Path("output_reports").mkdir(exist_ok=True)
loan_df.write_parquet(f"output_reports/loan_collateral_{reptyear}{reptmon}{reptday}.parquet")
loan2_df.write_parquet(f"output_reports/loan_maturity_{reptyear}{reptmon}{reptday}.parquet")
loan_df.write_csv(f"output_reports/loan_collateral_{reptyear}{reptmon}{reptday}.csv")
loan2_df.write_csv(f"output_reports/loan_maturity_{reptyear}{reptmon}{reptday}.csv")
