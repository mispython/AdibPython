#!/usr/bin/env python3
"""
EIMSCSST - Loan Transaction Fee Report Generator
Concise Python conversion from SAS using DuckDB and Polars
"""

import duckdb
import polars as pl
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

LOAN_DIR = Path("/data/loan")
LNTRN_DIR = Path("/data/lntrn")
ILOAN_DIR = Path("/data/iloan")
CTCS_DIR = Path("/data/ctcs")
DPTRN_DIR = Path("/data/dptrn")
FEE_DIR = Path("/data/fee")
OUTPUT_DIR = Path("/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(':memory:')

# ============================================================================
# GET REPORTING PERIOD
# ============================================================================

reptdate = pl.read_parquet(LOAN_DIR / "reptdate.parquet")["REPTDATE"][0]
REPTMON = f"{reptdate.month:02d}"
REPTYEAR = f"{reptdate.year % 100:02d}"

print(f"Report Period: {REPTMON}/{REPTYEAR}")

# ============================================================================
# PROCESS LOAN HISTORY
# ============================================================================

# Extract and filter transactions
lnhist = con.execute(f"""
    SELECT *, POSTDATE as TRANDT,
           ROUND(TOTAMT, 2) as TRANAMT
    FROM read_parquet('{LNTRN_DIR / "lnhist.parquet"}')
    WHERE ((TRANCODE IN (614,668) AND CHANNEL IN (513,514,523,524)) OR
           (TRANCODE IN (610,668) AND CHANNEL IN (505,515)))
    AND (REVERSED IS NULL OR REVERSED != 'R')
    ORDER BY ACCTNO, NOTENO, CHANNEL, POSTDATE, USERID, TIMECTRL
""").pl()

# Add ITEM classification and TRANTM
lnhist = lnhist.with_columns([
    pl.col("TIMECTRL").cast(pl.Utf8).str.zfill(6).str.to_time("%H%M%S").alias("TRANTM"),
    pl.when(pl.col("TRANCODE") == 614)
      .then(pl.when(pl.col("CHANNEL").is_in([513,514])).then(pl.lit("CDT")).otherwise(pl.lit("CRM")))
      .when((pl.col("TRANCODE") == 610) & (pl.col("CHANNEL").is_in([505,515])))
      .then(pl.lit("CDM"))
      .alias("ITEM")
])

# Split REDRAW and SSTC, then merge back with adjustment
redraw = lnhist.filter(pl.col("TRANCODE") == 668).select(
    ["ACCTNO", "NOTENO", "CHANNEL", "POSTDATE", "USERID", "TIMECTRL", 
     pl.col("TOTAMT").alias("RTOTAMT"), pl.col("TRANNO").alias("TTRANNO")]
)

sstc = lnhist.filter(pl.col("TRANCODE") != 668).join(
    redraw, on=["ACCTNO", "NOTENO", "CHANNEL", "POSTDATE", "USERID", "TIMECTRL"], how="left"
).with_columns(
    pl.when((pl.col("RTOTAMT") > 0) & ((pl.col("TTRANNO") - pl.col("TRANNO")) == 1))
      .then((pl.col("TOTAMT") + pl.col("RTOTAMT")).round(2))
      .otherwise(pl.col("TRANAMT"))
      .alias("TRANAMT")
).sort(["ACCTNO", "NOTENO", "TRANDT", "TRANAMT"])

# ============================================================================
# PROCESS CTCS AND DPTRX
# ============================================================================

ctcs = pl.read_parquet(CTCS_DIR / f"ctcs{REPTMON}.parquet").filter(
    pl.col("PRODIND") == "C"
).with_columns(
    pl.col("TRANAMT").round(2)
).sort(["TRANDT", "TRANAMT", "CHEQNO"])

# Load and merge deposit transactions
dptrx_list = []
for i in range(1, 5):
    file_path = DPTRN_DIR / f"dpbtran{REPTYEAR}{REPTMON}{i:02d}.parquet"
    if file_path.exists():
        dptrx_list.append(pl.read_parquet(file_path).filter(pl.col("TRANCODE") == 876))

if dptrx_list:
    dptrx = pl.concat(dptrx_list).select([
        pl.col("REPTDATE").alias("TRANDT"),
        pl.col("TRANAMT").round(2),
        pl.col("CHQNO").alias("CHEQNO"),
        pl.col("ACCTNO").alias("DEBACCT")
    ]).unique(subset=["TRANDT", "TRANAMT", "CHEQNO", "DEBACCT"])
    
    ctcs = ctcs.join(dptrx, on=["TRANDT", "TRANAMT", "CHEQNO"], how="left")

ctcs = ctcs.unique(subset=["ACCTNO", "NOTENO", "TRANDT", "TRANAMT"])

# ============================================================================
# CONSOLIDATE AND ADD LOAN NOTE INFO
# ============================================================================

conso = sstc.join(ctcs, on=["ACCTNO", "NOTENO", "TRANDT", "TRANAMT"], how="left").sort(
    ["ACCTNO", "NOTENO", "TRANDT", "TRANAMT"]
).with_columns(
    pl.col("ACCTNO").cum_count().over(["ACCTNO", "NOTENO", "TRANDT"]).alias("TRNSEQ")
)

# Load and merge loan notes
lnnote = pl.concat([
    pl.read_parquet(LOAN_DIR / "lnnote.parquet").select(
        ["ACCBRCH", "ACCTNO", "NOTENO", "LOANTYPE", "COSTCTR", "NTBRCH"]
    ),
    pl.read_parquet(ILOAN_DIR / "lnnote.parquet").select(
        ["ACCBRCH", "ACCTNO", "NOTENO", "LOANTYPE", "COSTCTR", "NTBRCH"]
    )
]).with_columns(
    pl.when((pl.col("NTBRCH").is_null()) | (pl.col("NTBRCH") == 0))
      .then(pl.col("ACCBRCH")).otherwise(pl.col("NTBRCH")).alias("NTBRCH")
).unique(subset=["ACCTNO", "NOTENO"])

conso = conso.join(lnnote, on=["ACCTNO", "NOTENO"], how="left").with_columns(
    pl.when(
        ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999)) |
        (pl.col("COSTCTR") == 4043) | (pl.col("COSTCTR") == 4048)
    ).then(pl.lit("PIB")).otherwise(pl.lit("PBB")).alias("ENTITY")
).sort(["ACCTNO", "NOTENO", "TRANDT", "TRNSEQ"])

# ============================================================================
# PROCESS AND MERGE FEES
# ============================================================================

fmfee = con.execute(f"""
    SELECT ACCTNO, NOTENO, ACTDATE as TRANDT, EFFDATE, TRANCODE,
           CASE WHEN TRANCODE = 820 THEN -TRANAMT ELSE TRANAMT END as FEEAMT
    FROM read_parquet('{FEE_DIR / f"feetran{REPTMON}4{REPTYEAR}.parquet"}')
    WHERE CHANNEL = 100 AND TRANCODE IN (760,820) AND FEEPLAN = 'FM'
    AND TRANAMT > 0 AND (REVERSED IS NULL OR REVERSED != 'R')
    ORDER BY ACCTNO, NOTENO, ACTDATE, TRANCODE
""").pl().with_columns(
    pl.col("ACCTNO").cum_count().over(["ACCTNO", "NOTENO", "TRANDT", "TRANCODE"]).alias("TRNSEQ")
).group_by(["ACCTNO", "NOTENO", "TRANDT", "TRNSEQ"]).agg(
    pl.col("FEEAMT").sum()
)

final = conso.join(fmfee, on=["ACCTNO", "NOTENO", "TRANDT", "TRNSEQ"], how="left").with_columns(
    pl.col("FEEAMT").fill_null(0)
)

# ============================================================================
# CALCULATE FEE CATEGORIES
# ============================================================================

final = final.with_columns([
    pl.when((pl.col("ITEM") == "CDM") & (pl.col("FEEAMT") > 0)).then(1).otherwise(0).alias("SVCCDM"),
    pl.when((pl.col("ITEM") == "CDM") & (pl.col("FEEAMT") > 0)).then(pl.col("TRANAMT")).otherwise(0).alias("SVCCDMAMT"),
    pl.when((pl.col("ITEM") == "CDM") & (pl.col("FEEAMT") <= 0)).then(1).otherwise(0).alias("WVECDM"),
    pl.when((pl.col("ITEM") == "CDM") & (pl.col("FEEAMT") <= 0)).then(pl.col("TRANAMT")).otherwise(0).alias("WVECDMAMT"),
    pl.when((pl.col("ITEM") != "CDM") & (pl.col("FEEAMT") > 0)).then(1).otherwise(0).alias("SVCFEE"),
    pl.when((pl.col("ITEM") != "CDM") & (pl.col("FEEAMT") > 0)).then(pl.col("TRANAMT")).otherwise(0).alias("SVCFEEAMT"),
    pl.when((pl.col("ITEM") != "CDM") & (pl.col("FEEAMT") <= 0)).then(1).otherwise(0).alias("WVEFEE"),
    pl.when((pl.col("ITEM") != "CDM") & (pl.col("FEEAMT") <= 0)).then(pl.col("TRANAMT")).otherwise(0).alias("WVEFEEAMT"),
]).sort(["TRANDT", "ACCTNO"])

# ============================================================================
# VALIDATION REPORT
# ============================================================================

fmvald = final.group_by("TRANDT").agg(pl.col("FEEAMT").sum()).sort("TRANDT")
print("\n" + "="*60)
print("VALIDATION - FEEAMT BY DATE")
print("="*60)
for row in fmvald.iter_rows(named=True):
    print(f"{row['TRANDT'].strftime('%d/%m/%Y'):>15}  {row['FEEAMT']:>20,.2f}")
print(f"{'TOTAL':>15}  {fmvald['FEEAMT'].sum():>20,.2f}\n")

# ============================================================================
# GENERATE REPORTS
# ============================================================================

def generate_report(entity):
    DLM = chr(0x05)
    temp = final.filter(pl.col("ENTITY") == entity)
    
    # Detail Report
    list_file = OUTPUT_DIR / f"LIST{entity}.txt"
    with open(list_file, 'w') as f:
        f.write(DLM.join(["BRANCH", "PAYMENT DATE", "TRANSACTION TIME", "ACCOUNT NUMBER",
                          "NOTE NUMBER", "TYPE OF TRANSACTIONS", "CHEQUE NUMBER",
                          "ACCOUNT DEBITED", "VALUE OF TRANSACTIONS (RM)", "TRANSACTION CODE",
                          "TRANSACTION SEQUENCE", "SERVICE FEE - REPAYMENT SST"]) + "\n")
        
        for row in temp.iter_rows(named=True):
            f.write(DLM.join([
                str(row["NTBRCH"] or ""),
                row["TRANDT"].strftime("%d/%m/%Y") if row["TRANDT"] else "",
                row["TRANTM"].strftime("%H:%M:%S") if row["TRANTM"] else "",
                str(row["ACCTNO"] or ""),
                str(row["NOTENO"] or ""),
                str(row["ITEM"] or ""),
                str(row["CHEQNO"] or ""),
                str(row["DEBACCT"] or ""),
                f"{row['TRANAMT']:,.2f}" if row["TRANAMT"] else "0.00",
                str(row["TRANCODE"] or ""),
                str(row["TRANNO"] or ""),
                f"{row['FEEAMT']:,.2f}" if row["FEEAMT"] else "0.00"
            ]) + "\n")
    
    # Summary Report
    summ = temp.sort("NTBRCH").group_by("NTBRCH").agg([
        pl.col("SVCFEE").sum(), pl.col("WVEFEE").sum(),
        pl.col("SVCCDM").sum(), pl.col("WVECDM").sum(),
        pl.col("SVCFEEAMT").sum(), pl.col("WVEFEEAMT").sum(),
        pl.col("SVCCDMAMT").sum(), pl.col("WVECDMAMT").sum()
    ]).sort("NTBRCH")
    
    summ_file = OUTPUT_DIR / f"SUMM{entity}.txt"
    with open(summ_file, 'w') as f:
        f.write(DLM.join(["BRANCH", "CDT/CRM", "", "", "", "CDM", "", "", "",
                          "SST Charges Sub Total", "", "SST Waived Sub Total", "",
                          "TOTAL", ""]) + "\n")
        f.write(DLM.join(["", "Charged", "Amount", "Waived", "Amount", "Charged", "Amount",
                          "Waived", "Amount", "No", "Amount", "No", "Amount", "No", "Amount"]) + "\n")
        
        totals = {"svcfee": 0, "svcfeeamt": 0, "wvefee": 0, "wvefeeamt": 0,
                  "svccdm": 0, "svccdmamt": 0, "wvecdm": 0, "wvecdmamt": 0}
        
        for row in summ.iter_rows(named=True):
            svctot = row["SVCFEE"] + row["SVCCDM"]
            svctotamt = row["SVCFEEAMT"] + row["SVCCDMAMT"]
            wvetot = row["WVEFEE"] + row["WVECDM"]
            wvetotamt = row["WVEFEEAMT"] + row["WVECDMAMT"]
            
            f.write(DLM.join([
                str(row["NTBRCH"] or ""),
                f"{row['SVCFEE']:,}", f"{row['SVCFEEAMT']:,.2f}",
                f"{row['WVEFEE']:,}", f"{row['WVEFEEAMT']:,.2f}",
                f"{row['SVCCDM']:,}", f"{row['SVCCDMAMT']:,.2f}",
                f"{row['WVECDM']:,}", f"{row['WVECDMAMT']:,.2f}",
                f"{svctot:,}", f"{svctotamt:,.2f}",
                f"{wvetot:,}", f"{wvetotamt:,.2f}",
                f"{svctot + wvetot:,}", f"{svctotamt + wvetotamt:,.2f}"
            ]) + "\n")
            
            for k in totals: totals[k] += row[k.upper()]
        
        tsvctot = totals["svcfee"] + totals["svccdm"]
        tsvctotamt = totals["svcfeeamt"] + totals["svccdmamt"]
        twvetot = totals["wvefee"] + totals["wvecdm"]
        twvetotamt = totals["wvefeeamt"] + totals["wvecdmamt"]
        
        f.write(DLM.join([
            "TOTAL",
            f"{totals['svcfee']:,}", f"{totals['svcfeeamt']:,.2f}",
            f"{totals['wvefee']:,}", f"{totals['wvefeeamt']:,.2f}",
            f"{totals['svccdm']:,}", f"{totals['svccdmamt']:,.2f}",
            f"{totals['wvecdm']:,}", f"{totals['wvecdmamt']:,.2f}",
            f"{tsvctot:,}", f"{tsvctotamt:,.2f}",
            f"{twvetot:,}", f"{twvetotamt:,.2f}",
            f"{tsvctot + twvetot:,}", f"{tsvctotamt + twvetotamt:,.2f}"
        ]) + "\n")
    
    print(f"Generated: {list_file.name}, {summ_file.name}")

# Generate reports for both entities
generate_report("PBB")
generate_report("PIB")

con.close()
print("\nCompleted successfully.")
