import duckdb
import polars as pl
from datetime import datetime

# paths
BASE_PATH = "/data"
INPUT_PATH = f"{BASE_PATH}/input"
OUTPUT_PATH = f"{BASE_PATH}/output"

REPTDATE_FILE = f"{INPUT_PATH}/loan_reptdate.parquet"
OUTFILE = f"{OUTPUT_PATH}/EIFMNP2B.txt"
RPSFILE = f"{OUTPUT_PATH}/EIFMNP2B_RPS.txt"

con = duckdb.connect()

# read report date
rept = pl.from_arrow(con.execute(f"SELECT * FROM '{REPTDATE_FILE}'").arrow())
reptdate = rept.select(pl.col("REPTDATE")).to_series()[0]

rept_mon = f"{reptdate.month:02d}"
prev_mon = 12 if reptdate.month == 1 else reptdate.month - 1
prev_mon = f"{prev_mon:02d}"
rdate = reptdate.strftime("%d/%m/%Y")

this_path = f"{INPUT_PATH}/loan2m{rept_mon}.parquet"
last_path = f"{INPUT_PATH}/loan2m{prev_mon}.parquet"

thismth = pl.from_arrow(con.execute(f"SELECT * FROM '{this_path}'").arrow())
lastmth = pl.from_arrow(con.execute(f"SELECT * FROM '{last_path}'").arrow())

# new accounts
newca = (
    thismth.join(lastmth.select("ACCTNO"), on="ACCTNO", how="anti")
    .with_columns([
        pl.col("COLLDESC").str.slice(0,14).alias("MAKE"),
        pl.col("COLLDESC").str.slice(14,21).alias("MODEL")
    ])
)

# subtotal
totname = (
    newca.group_by("GUAREND")
    .agg(pl.col("NETBAL").sum().alias("TOTNET"))
)

grand = totname.select(pl.col("TOTNET").sum().alias("GRANDTOT"))

totname = totname.with_columns([
    pl.lit("SUB-TOTAL").alias("SUBTOT"),
    pl.col("TOTNET").alias("NETBAL")
])

grand = grand.with_columns([
    pl.lit("GRAND TOTAL").alias("SUBTOT"),
    pl.col("GRANDTOT").alias("NETBAL")
])

# loanname
loanname = (
    newca.join(totname.select(["GUAREND","TOTNET"]), on="GUAREND", how="left")
    .with_columns([
        pl.col("COLLDESC").str.slice(0,14).alias("MAKE"),
        pl.col("COLLDESC").str.slice(14,21).alias("MODEL"),
        pl.lit(rdate).alias("REPTDATE")
    ])
)

totname2 = pl.concat([loanname, totname])
totname2 = pl.concat([totname2, grand])

totname2 = totname2.sort(
    ["TOTNET","GUAREND","SUBTOT"],
    descending=[True,False,False]
)

# report output
with open(OUTFILE,"w") as f:
    header = f"PUBLIC BANK - (NEWLY SURFACE CA) AS AT {rdate}"
    f.write(header+"\n")

    f.write(
        f"{'A/C NO.':<18}{'NOTE NO.':<10}{'NAME':<36}"
        f"{'BRANCH':<9}{'DAYS':<6}{'BORSTAT':<10}"
        f"{'ISSUE DATE':<15}{'NETBAL':>15}{'CAC NAME':<20}"
        f"{'REGION OFF':<20}{'MAKE':<15}{'MODEL':<20}"
        f"{'DEALER NO':<12}{'DEALER NAME':<25}"
        f"{'ORI BRANCH':<12}{'CUSTFISS':<10}\n"
    )

    for r in totname2.iter_rows(named=True):
        f.write(
            f"{str(r.get('ACCTNO','')):<18}"
            f"{str(r.get('NOTENO','')):<10}"
            f"{str(r.get('NAME','')):<36}"
            f"{str(r.get('BRANCH','')):<9}"
            f"{str(r.get('DAYS','')):<6}"
            f"{str(r.get('BORSTAT','')):<10}"
            f"{str(r.get('ISSDTE','')):<15}"
            f"{r.get('NETBAL',0):>15,.2f}"
            f"{str(r.get('CACNAME','')):<20}"
            f"{str(r.get('REGION','')):<20}"
            f"{str(r.get('MAKE','')):<15}"
            f"{str(r.get('MODEL','')):<20}"
            f"{str(r.get('DEALERNO','')):<12}"
            f"{str(r.get('COMPNAME','')):<25}"
            f"{str(r.get('PRIMOFHP','')):<12}"
            f"{str(r.get('CUSTFISS','')):<10}\n"
        )

# RPS report
rps2m = (
    newca.with_columns([
        pl.col("COLLDESC").str.slice(0,14).alias("MAKE"),
        pl.col("COLLDESC").str.slice(14,21).alias("MODEL"),
        pl.lit(rdate).alias("REPTDATE")
    ])
    .sort(["BRANCH","NETBAL"], descending=[False,True])
)

linecnt = 0
pagecnt = 0
current_branch = None

with open(RPSFILE,"w") as f:

    for r in rps2m.iter_rows(named=True):

        if r["BRANCH"] != current_branch or linecnt > 55:
            pagecnt += 1
            linecnt = 7
            current_branch = r["BRANCH"]

            f.write(f"BRANCH : {r['BRANCH']:<20}P U B L I C   B A N K   B E R H A D{'':<40}PAGE NO : {pagecnt}\n")
            f.write(f"REPORT NO : 2MTHNPLNEW{'':<20}NEWLY SURFACE CLOSE ATTENTION{'':<5}{rdate}\n\n")
            f.write(
                f"{'A/C NO.':<18}{'NOTE NO.':<10}{'NAME':<36}"
                f"{'BRANCH':<9}{'DAYS':<6}{'BORSTAT':<10}"
                f"{'ISSUE DATE':<15}{'NETBAL':>15}"
                f"{'MAKE':<15}{'MODEL':<20}\n"
            )

        f.write(
            f"{str(r.get('ACCTNO','')):<18}"
            f"{str(r.get('NOTENO','')):<10}"
            f"{str(r.get('NAME','')):<36}"
            f"{str(r.get('BRANCH','')):<9}"
            f"{str(r.get('DAYS','')):<6}"
            f"{str(r.get('BORSTAT','')):<10}"
            f"{str(r.get('ISSDTE','')):<15}"
            f"{r.get('NETBAL',0):>15,.2f}"
            f"{str(r.get('MAKE','')):<15}"
            f"{str(r.get('MODEL','')):<20}\n"
        )

        linecnt += 1
