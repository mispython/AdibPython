#!/usr/bin/env python3
"""
EIBMCRIS - BNM Credit Information Reporting System
Concise Python conversion from SAS using DuckDB and Polars
"""

import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

LOAN_DIR = Path("/data/loan")
LNADDR_DIR = Path("/data/lnaddr")
BNM_DIR = Path("/data/bnm")
MNICS_DIR = Path("/data/mnics")
OD_DIR = Path("/data/od")
ODADDR_DIR = Path("/data/odaddr")
DEPOSIT_FILE = Path("/data/deposit/gp3.txt")
OUTPUT_DIR = Path("/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Format mappings (PBBCRFMT)
PAYFQ = {'1':'M', '2':'Q', '3':'S', '4':'A', '5':'B', '9':'I'}
RISK = {1:'C', 2:'S', 3:'D', 4:'B'}
LNPRODC = {}  # Add your product code mappings

fmt_payfq = lambda v: PAYFQ.get(str(v), 'O')
fmt_risk = lambda v: RISK.get(v, 'P')
fmt_lnprodc = lambda v: LNPRODC.get(v, str(v).zfill(5))

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = pl.read_parquet(LOAN_DIR / "reptdate.parquet")["REPTDATE"][0]
NOWK = {8:'1', 15:'2', 22:'3'}.get(reptdate.day, '4')
REPTYEAR, REPTMON, REPTDAY = f"{reptdate.year}", f"{reptdate.month:02d}", f"{reptdate.day:02d}"
REPTDTE = f"{REPTDAY}{REPTMON}{REPTYEAR}"
REPTDATE = int((reptdate - datetime(1960, 1, 1)).days)

print(f"Report Date: {REPTDAY}/{REPTMON}/{REPTYEAR}, Week: {NOWK}")

# ============================================================================
# LOAD DATA
# ============================================================================

# Loan address data
lnaddr = pl.read_parquet(LOAN_DIR / "lnnote.parquet").join(
    pl.read_parquet(LNADDR_DIR / "lnname.parquet"), on="ACCTNO", how="inner"
).select([
    "ACCTNO", 
    pl.col("TAXNO").alias("OLDIC"), 
    pl.col("GUAREND").alias("NEWIC"),
    "NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", "NAMELN5"
])

# Load and merge loans
loan = pl.concat([
    pl.read_parquet(BNM_DIR / f"loan{REPTMON}{NOWK}.parquet"),
    pl.read_parquet(BNM_DIR / f"uloan{REPTMON}{NOWK}.parquet").with_columns(
        pl.col("COMMNO").alias("NOTENO")
    )
], how="diagonal").filter(
    ~pl.col("CUSTCODE").is_in([1, 2, 3, 10, 11, 12, 81, 91])
).sort("ACCTNO").with_columns(
    (pl.col("ACCTNO").cum_count().over("ACCTNO") == 1).alias("first_acct")
).filter(
    pl.when(pl.col("PRODCD").is_in(['34190', '34690']))
      .then(pl.col("first_acct")).otherwise(True)
).drop("first_acct")

# Aggregate limits
temp = loan.group_by("ACCTNO").agg([
    pl.col("APPRLIMT").sum().alias("TOTLIMT"),
    pl.count().alias("LOANCNT")
])

# Load GP3 fixed-width file
with open(DEPOSIT_FILE, 'r') as f:
    gp3klu = pl.DataFrame([{
        'BRANCH': int(ln[0:3]), 'ACCTNO': int(ln[3:13]), 'ZERO5': int(ln[13:18]),
        'RPTDAY': int(ln[18:20]), 'RPTMON': int(ln[20:22]), 'RPTYEAR': int(ln[22:26]),
        'SCV': float(ln[26:38]), 'IIS': float(ln[38:50]), 'IISR': float(ln[50:62]),
        'IISW': float(ln[62:74]), 'PRINWOFF': float(ln[74:86]), 'GP3IND': ln[86:87]
    } for ln in f]).with_columns(pl.lit(None).alias("NOTENO")).sort(["ACCTNO", "NOTENO"])

# ============================================================================
# PROCESS O/D DAYS OVERDUE
# ============================================================================

def parse_dt(dt_int):
    if not dt_int or dt_int == 0: return None
    s = str(dt_int).zfill(11)
    try: return datetime(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except: return None

loan2 = pl.read_parquet(BNM_DIR / f"loan{REPTMON}{NOWK}.parquet").filter(
    (pl.col("ACCTYPE") == "OD") & ((pl.col("APPRLIMT") >= 0) | (pl.col("BALANCE") < 0)) &
    ((pl.col("ACCTNO") <= 3900000000) | (pl.col("ACCTNO") > 3999999999))
).select(["ACCTNO", "BRANCH", "BALANCE", "PRODUCT"]).unique("ACCTNO")

od = pl.read_parquet(OD_DIR / "overdft.parquet").filter(
    (pl.col("EXCESSDT") > 0) | (pl.col("TODDATE") > 0)
).select(["ACCTNO", "EXCESSDT", "TODDATE", "RISKCODE"]).unique("ACCTNO")

loanod = loan2.join(od, on="ACCTNO", how="inner").filter(
    (pl.col("BRANCH").is_not_null()) & (~pl.col("PRODUCT").is_in([517, 500]))
).with_columns([
    pl.col("EXCESSDT").map_elements(parse_dt, return_dtype=pl.Datetime).alias("EXCDATE"),
    pl.col("TODDATE").map_elements(parse_dt, return_dtype=pl.Datetime).alias("TODDT")
]).with_columns(
    pl.when((pl.col("EXCESSDT") != 0) & (pl.col("TODDATE") != 0))
      .then(pl.when(pl.col("EXCDATE") <= pl.col("TODDT")).then(pl.col("EXCDATE")).otherwise(pl.col("TODDT")))
      .when(pl.col("EXCESSDT") > 0).then(pl.col("EXCDATE"))
      .when(pl.col("TODDATE") > 0).then(pl.col("TODDT"))
      .otherwise(None).alias("BLDATE")
).with_columns(
    pl.when(pl.col("BLDATE").is_not_null())
      .then((pl.lit(reptdate) - pl.col("BLDATE").cast(pl.Date)).dt.total_days() + 1)
      .otherwise(None).cast(pl.Int32).alias("DAYS")
).select(["ACCTNO", "DAYS"])

# ============================================================================
# BUILD FINAL LOAN DATASET
# ============================================================================

addr = pl.concat([
    lnaddr, pl.read_parquet(ODADDR_DIR / "savings.parquet")
], how="diagonal").select(["ACCTNO", "NAMELN2", "NAMELN3", "NAMELN4", "NAMELN5", "OLDIC", "NEWIC"])

cis = pl.read_parquet(MNICS_DIR / "cis.parquet").unique("ACCTNO").select(["ACCTNO", "NAME", "SIC"])

loan = loan.join(temp, on="ACCTNO", how="left").filter(
    (pl.col("TOTLIMT") >= 1000000) | (pl.col("LOANSTAT") == 3)
).sort(["ACCTNO", "NOTENO"]).join(
    gp3klu, on=["ACCTNO", "NOTENO"], how="left"
).with_columns([
    pl.lit(REPTDTE).alias("REPTDTE"),
    pl.when(pl.col("ACCTYPE") != "OD")
      .then(pl.col("BALANCE") - pl.col("CURBAL") - pl.col("FEEAMT"))
      .otherwise(None).alias("INTOUT")
]).drop("NAME").join(
    addr, on="ACCTNO", how="left"
).join(
    cis, on="ACCTNO", how="left"
).join(
    loanod, on="ACCTNO", how="left"
).with_columns([
    pl.when(pl.col("NEWIC").str.strip_chars("0 ") == "").then(" ").otherwise(pl.col("NEWIC")).alias("NEWIC"),
    pl.when(pl.col("OLDIC").str.strip_chars("0 ") == "").then(" ").otherwise(pl.col("OLDIC")).alias("OLDIC"),
    pl.col("NAMELN2").alias("ADDR1"),
    (pl.col("NAMELN3").str.strip_chars() + " " + pl.col("NAMELN4").str.strip_chars() + 
     " " + pl.col("NAMELN5").fill_null("")).str.strip_chars().alias("ADDR2"),
    pl.when((pl.col("ACCTNO") >= 2990000000) & (pl.col("ACCTNO") <= 2999999999))
      .then("I").otherwise("C").alias("FITYPE"),
    pl.when(pl.col("LOANCNT") > 1).then("M").otherwise("S").alias("LOANOPT"),
    pl.col("APPRDATE").dt.strftime("%d%m%Y").alias("APPRDTE")
]).with_columns(
    pl.when((pl.col("ACCTYPE") == "OD") & (pl.col("CLOSEDTE") > 0))
      .then(pl.col("CLOSEDTE").cast(pl.Date).dt.strftime("%d%m%Y"))
      .when((pl.col("ACCTYPE") == "LN") & (pl.col("PAIDIND") == "P") & 
            (pl.col("BALANCE") <= 0) & (pl.col("CLOSEDTE") > 0))
      .then(pl.col("CLOSEDTE").cast(pl.Date).dt.strftime("%d%m%Y"))
      .otherwise("").alias("CLOSDTE")
).with_columns([
    pl.when(pl.col("ACCTYPE") == "LN")
      .then(pl.col("PRODUCT").map_elements(fmt_lnprodc, return_dtype=pl.Utf8))
      .otherwise("34110").alias("PRODCD"),
    pl.when(pl.col("ACCTYPE") == "LN")
      .then(pl.col("PAYFREQ").cast(pl.Utf8).map_elements(fmt_payfq, return_dtype=pl.Utf8))
      .otherwise("A").alias("PAYTERM"),
    pl.when((pl.col("ACCTYPE") == "LN") & pl.col("BLDATE").is_not_null() & (pl.col("BALANCE") >= 1))
      .then(pl.lit(REPTDATE) - pl.col("BLDATE").cast(pl.Date).cast(pl.Int32))
      .otherwise(pl.col("DAYS")).alias("DAYS")
]).with_columns([
    pl.when(pl.col("SECURE") != "S").then("C").otherwise(pl.col("SECURE")).alias("SECURE"),
    pl.when(pl.col("PRODUCT").is_in([225, 226])).then("C").otherwise("N").alias("CAGAMAS"),
    pl.col("RISKRTE").map_elements(fmt_risk, return_dtype=pl.Utf8).alias("RISKC"),
    (pl.col("DAYS") / 30.00050).floor().cast(pl.Int32).alias("MTHARR")
]).with_columns(
    pl.when(pl.col("BORSTAT") == "F").then(999)
      .when(pl.col("BORSTAT") == "W").then(0)
      .otherwise(pl.col("MTHARR")).alias("MTHARR")
).unique(["BRANCH", "ACCTNO", "NOTENO"]).sort(["BRANCH", "ACCTNO", "NOTENO"])

loan.write_parquet(OUTPUT_DIR / "loan.parquet")

# ============================================================================
# PAGE 02: DIRECTOR INFO
# ============================================================================

dirinfo = loan.unique("ACCTNO").select(["BRANCH", "ACCTNO", "REPTDTE"]).join(
    pl.read_parquet(MNICS_DIR / "cis.parquet"), on="ACCTNO", how="left"
).with_columns([
    pl.when(pl.col("DIRNIC").str.strip_chars("0 ") == "").then(" ").otherwise(pl.col("DIRNIC")).alias("DIRNIC"),
    pl.when(pl.col("DIROIC").str.strip_chars("0 ") == "").then(" ").otherwise(pl.col("DIROIC")).alias("DIROIC")
]).unique(["BRANCH", "ACCTNO"]).sort(["BRANCH", "ACCTNO"])

with open(OUTPUT_DIR / "PAGE02.txt", 'w') as f:
    for r in dirinfo.iter_rows(named=True):
        f.write(f"0233{r['BRANCH']:05d}{r['ACCTNO']:10d}{r['REPTDTE']:8}"
                f"{r.get('DIRNIC',''):20}{r.get('DIROIC',''):20}  "
                f"{r.get('DIRNAME',''):60}MYN\n")

print(f"PAGE02: {dirinfo.height} records")

# ============================================================================
# PAGE 01: BORROWER/LOAN PROFILE
# ============================================================================

page01 = loan.unique("ACCTNO", maintain_order=True)

with open(OUTPUT_DIR / "PAGE01.txt", 'w') as f:
    for r in page01.iter_rows(named=True):
        f.write(f"0233{r['BRANCH']:05d}{r['ACCTNO']:10d}{r['REPTDTE']:8}"
                f"{r.get('NEWIC',''):20}{r.get('OLDIC',''):20}  {r.get('NAME',''):60}"
                f"{r.get('ADDR1',''):45}{r.get('ADDR2',''):90}{r.get('CUSTCD',''):2}MY"
                f"{r.get('SIC',0):04d}NN{r.get('FITYPE','C'):1}{r.get('LOANOPT','S'):1}"
                f"{r.get('TOTLIMT',0):12.0f}{r.get('LOANCNT',0):2d}N\n")

print(f"PAGE01: {page01.height} records")

# ============================================================================
# PAGE 03: LOAN DETAILS
# ============================================================================

with open(OUTPUT_DIR / "PAGE03.txt", 'w') as f:
    for r in loan.iter_rows(named=True):
        f.write(f"0233{r['BRANCH']:05d}{r['ACCTNO']:10d}{r.get('NOTENO',0):05d}"
                f"{r['REPTDTE']:8}{r.get('PRODCD',''):5}{r.get('APPRDTE',''):8}"
                f"{r.get('CLOSDTE',''):8}          {r.get('SECTORCD',''):4}"
                f"{r.get('NOTETERM',0):3d}{r.get('PAYTERM','A'):1}"
                f"{r.get('MTHARR',0):3d}{r.get('MTHARR',0):3d}{r.get('RISKC','P'):1}"
                f"{r.get('SECURE','C'):1}NNN{r.get('CAGAMAS','N'):1}SNNNMYR"
                f"{r.get('CURBAL',0):12.0f}{r.get('INTOUT',0):12.0f}"
                f"{r.get('FEEAMT',0):12.0f}{r.get('BALANCE',0):12.0f}N\n")

print(f"PAGE03: {loan.height} records")

# ============================================================================
# PAGE 04: GP3 DATA
# ============================================================================

page04 = gp3klu.with_columns([
    pl.col("IISR").fill_null(0), pl.col("IISW").fill_null(0), 
    pl.col("IIS").fill_null(0), pl.lit(int(REPTDTE)).alias("REPTDATE")
]).sort(["BRANCH", "ACCTNO"])

with open(OUTPUT_DIR / "PAGE04.txt", 'w') as f:
    for r in page04.iter_rows(named=True):
        f.write(f"{r['BRANCH']:03d}{r['ACCTNO']:010d}{r['ZERO5']:05d}"
                f"{r['REPTDATE']:08d}{r['SCV']:012.0f}{r['IIS']:012.2f}"
                f"{r['IISR']:012.2f}{r['IISW']:012.2f}{r['PRINWOFF']:012.2f}"
                f"{r['GP3IND']:1}\n")

print(f"PAGE04: {page04.height} records")

# ============================================================================
# EXCEPTIONAL REPORT
# ============================================================================

exclude = page04.select("ACCTNO").join(
    pl.read_parquet(OD_DIR / "overdft.parquet").filter(
        (pl.col("EXCESSDT") > 0) | (pl.col("TODDATE") > 0)
    ).select(["ACCTNO", "EXCESSDT", "TODDATE", "RISKCODE"]), on="ACCTNO", how="inner"
).with_columns(
    (pl.col("ACCTNO").cum_count().over("ACCTNO") > 1).alias("is_dup")
).filter(pl.col("is_dup")).drop("is_dup")

if exclude.height > 0:
    print(f"\nEXCEPTIONAL O/D: {exclude.height} records")
    exclude.write_csv(OUTPUT_DIR / "EXCLUDE_OD.csv")

print(f"\nCompleted: PAGE01-04 generated in {OUTPUT_DIR}")
