import polars as pl
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
DEPO_PATH = BASE_PATH / "depo"
CARD_PATH = BASE_PATH / "card"
CISDP_PATH = BASE_PATH / "cisdp"
CISSAFD_PATH = BASE_PATH / "cissafd"
PDEPO_PATH = BASE_PATH / "pdepo"
PIDEPO_PATH = BASE_PATH / "pidepo"
IDEPO_PATH = BASE_PATH / "idepo"
OUTPUT_PATH = BASE_PATH / "output"

conn = duckdb.connect()

# ==================== REPTDATE PROCESSING ====================
def process_reptdate():
    reptdate_df = pl.read_parquet(DEPO_PATH / "REPTDATE.parquet")
    reptdate_dt = reptdate_df[0, "REPTDATE"].date()
    
    day_val = reptdate_dt.day
    month_val = reptdate_dt.month
    year_val = reptdate_dt.year
    
    RDATE = reptdate_dt
    RDATE1 = RDATE - timedelta(days=1)
    
    if 2 <= day_val <= 9:
        reptdate_adj = datetime(year_val, month_val, 1).date() - timedelta(days=1)
        WK = "04"
    elif 10 <= day_val <= 16:
        reptdate_adj = datetime(year_val, month_val, 8).date()
        WK = "01"
    elif 17 <= day_val <= 23:
        reptdate_adj = datetime(year_val, month_val, 15).date()
        WK = "02"
    else:
        if day_val == 1:
            reptdate_adj = datetime(year_val, month_val, 1).date() - timedelta(days=1)
        else:
            reptdate_adj = datetime(year_val, month_val, 22).date()
        WK = "03"
    
    REPTDAY = f"{day_val:02d}"
    REPTMON = f"{month_val:02d}"
    REPTYEAR = str(year_val)[-2:]
    NOWK = WK
    RDTEA = RDATE.strftime("%d%m%Y")
    RDTEB = RDATE1.strftime("%d%m%Y")
    
    return REPTDAY, REPTMON, REPTYEAR, NOWK, RDTEA, RDTEB

REPTDAY, REPTMON, REPTYEAR, NOWK, RDTEA, RDTEB = process_reptdate()

# ==================== CARD DATA PROCESSING ====================
card_file = CARD_PATH / f"UNICARD{REPTYEAR}{REPTMON}{NOWK}.parquet"
card_df = pl.read_parquet(card_file)

# Filter card data
card_filtered = card_df.filter(
    (pl.col("CLOSECD").is_null() | (pl.col("CLOSECD") == "")) &
    (pl.col("ACCTYPE") != "IS") &
    ((pl.col("ACCTYPE") != "IA") | (pl.col("CARDHOLD") == 1))
).with_columns(
    pl.when(pl.col("NEWIC").is_null() | (pl.col("NEWIC") == ""))
    .then(pl.col("OLDIC"))
    .otherwise(pl.col("NEWIC"))
    .alias("NEWIC")
)

# Split card data
card_main = card_filtered.filter(
    pl.col("MONITOR").is_in(["Z", "I"]) | (pl.col("SOURCE") == "GCPIFD0209")
).select(["CARDNO", "MONITOR", "SOURCE", "CLOSECD", "NEWIC", "OLDIC", "CUSTNAME", "APPRLIMT"])

card_z = card_filtered.filter(pl.col("MONITOR") == "Z").select(["NEWIC"])

card_main = card_main.unique(subset=["NEWIC"])
card_z = card_z.unique(subset=["NEWIC"])

# ==================== CIS DATA PROCESSING ====================
def load_cis_data(filepath, keep_cols):
    return pl.read_parquet(filepath).select(keep_cols)

cisca = load_cis_data(CISDP_PATH / "DEPOSIT.parquet", ["ACCTNO", "NEWIC"])
cissafd = load_cis_data(CISSAFD_PATH / "DEPOSIT.parquet", ["ACCTNO", "NEWIC"])

# Merge with card data
cisca_merged = card_main.join(cisca, on="NEWIC", how="inner").with_columns(pl.lit("CA").alias("TYPE"))

# Split CISSAFD data
cissafd_merged = card_main.join(cissafd, on="NEWIC", how="inner")

cisfd = cissafd_merged.filter(
    ((1000000000 < pl.col("ACCTNO")) & (pl.col("ACCTNO") < 2000000000)) |
    ((7000000000 < pl.col("ACCTNO")) & (pl.col("ACCTNO") < 8000000000))
).with_columns(pl.lit("FD").alias("TYPE"))

cissa = cissafd_merged.filter(
    (4000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") < 7000000000)
).with_columns(pl.lit("SA").alias("TYPE"))

# Sort CIS datasets
cisca_sorted = cisca_merged.sort("ACCTNO")
cisfd_sorted = cisfd.sort("ACCTNO")
cissa_sorted = cissa.sort("ACCTNO")

# ==================== DEPOSIT DATA PROCESSING ====================
def load_and_merge_deposits(cis_df, deposit_paths, rename_col=None):
    """Load and merge deposit data"""
    dfs = []
    for path in deposit_paths:
        df = pl.read_parquet(path).select(["ACCTNO", "CURBAL"])
        if rename_col:
            df = df.rename({"CURBAL": rename_col})
        dfs.append(df)
    
    combined = pl.concat(dfs).unique(subset=["ACCTNO"])
    merged = cis_df.join(combined, on="ACCTNO", how="left")
    return merged

# Previous day deposits
pdepo_paths = [
    PDEPO_PATH / "SAVING.parquet",
    PDEPO_PATH / "CURRENT.parquet", 
    PDEPO_PATH / "FD.parquet",
    PIDEPO_PATH / "SAVING.parquet",
    PIDEPO_PATH / "CURRENT.parquet",
    PIDEPO_PATH / "FD.parquet"
]

pdepo_sa = load_and_merge_deposits(cissa_sorted, 
                                   [p for p in pdepo_paths if "SAVING" in str(p)], 
                                   "PRE_CURBAL")
pdepo_ca = load_and_merge_deposits(cisca_sorted, 
                                   [p for p in pdepo_paths if "CURRENT" in str(p)], 
                                   "PRE_CURBAL")
pdepo_fd = load_and_merge_deposits(cisfd_sorted, 
                                   [p for p in pdepo_paths if "FD" in str(p)], 
                                   "PRE_CURBAL")

# Current day deposits
depo_paths = [
    DEPO_PATH / "SAVING.parquet",
    DEPO_PATH / "CURRENT.parquet",
    DEPO_PATH / "FD.parquet",
    IDEPO_PATH / "SAVING.parquet",
    IDEPO_PATH / "CURRENT.parquet",
    IDEPO_PATH / "FD.parquet"
]

depo_sa = load_and_merge_deposits(cissa_sorted, 
                                  [p for p in depo_paths if "SAVING" in str(p)], 
                                  "CURBAL")
depo_ca = load_and_merge_deposits(cisca_sorted, 
                                  [p for p in depo_paths if "CURRENT" in str(p)], 
                                  "CURBAL")
depo_fd = load_and_merge_deposits(cisfd_sorted, 
                                  [p for p in depo_paths if "FD" in str(p)], 
                                  "CURBAL")

# Combine all deposit data
depo_all = pl.concat([depo_sa, depo_ca, depo_fd]).sort("ACCTNO")
pdepo_all = pl.concat([pdepo_sa, pdepo_ca, pdepo_fd]).sort("ACCTNO")

# Merge current and previous deposits
depo_merged = depo_all.join(pdepo_all.select(["ACCTNO", "PRE_CURBAL"]), 
                           on="ACCTNO", 
                           how="left").with_columns(
    pl.when((pl.col("PRE_CURBAL") - pl.col("CURBAL")) > 0)
    .then(pl.col("PRE_CURBAL") - pl.col("CURBAL"))
    .otherwise(0)
    .alias("WITHDR")
)

# Summarize by NEWIC
depo_summary = depo_merged.group_by("NEWIC").agg([
    pl.col("PRE_CURBAL").sum(),
    pl.col("CURBAL").sum(),
    pl.col("WITHDR").sum()
])

# Calculate percentage
depo_summary = depo_summary.with_columns(
    (pl.col("WITHDR") / pl.col("PRE_CURBAL") * 100).alias("PERCEN")
)

# Split based on criteria
depo2 = depo_summary.filter(pl.col("PERCEN") >= 50).select(["NEWIC", pl.col("CURBAL").alias("SUMBAL")])
depo3 = depo_summary.filter((pl.col("PERCEN") < 50) & (pl.col("CURBAL") <= 500000)).select(["NEWIC", pl.col("CURBAL").alias("SUMBAL")])

# Merge depo3 with card_z
depo3a = depo3.join(card_z, on="NEWIC", how="inner")

# Combine final selection
tot = pl.concat([depo2, depo3a]).unique(subset=["NEWIC"])

# Final merge
final = depo_merged.join(tot, on="NEWIC", how="inner").sort(["CUSTNAME", "ACCTNO"])

# ==================== REPORT GENERATION ====================
def generate_report(final_df, rdateb, rdatea):
    """Generate formatted report"""
    report_lines = []
    
    # Add ASA page control (form feed)
    report_lines.append("\x0c" + " " * 30 + "P U B L I C   B A N K   B E R H A D")
    report_lines.append(" " * 30 + "REPORT PERIOD : " + 
                       f"{rdateb[:2]}/{rdateb[2:4]}/{rdateb[4:]} - " +
                       f"{rdatea[:2]}/{rdatea[2:4]}/{rdatea[4:]}")
    report_lines.append(" " * 35 + "CARDHOLDERS DEPOSITS ACCOUNT")
    report_lines.append("")
    report_lines.append("=" * 120)
    
    # Header
    header = (
        f"{'CUSTOMER NAME':<27} "
        f"{'NEW ICNO':<12} "
        f"{'OLD ICNO':<12} "
        f"{'CARD NUMBER':<16} "
        f"{'CREDIT CARD LIMIT':<18} "
        f"{'CO':<4} "
        f"{'TY':<4} "
        f"{'ACCTNO':>13} "
        f"{'BALANCE':>15}"
    )
    report_lines.append(header)
    report_lines.append("-" * 120)
    
    # Group by CUSTNAME and add data
    current_cust = None
    for row in final_df.iter_rows(named=True):
        custname = row["CUSTNAME"][:27] if row["CUSTNAME"] else ""
        
        if custname != current_cust:
            if current_cust is not None:
                # Add summary line for previous customer
                report_lines.append("-" * 120)
            current_cust = custname
        
        # Format row
        row_line = (
            f"{custname:<27} "
            f"{row.get('NEWIC', ''):<12} "
            f"{row.get('OLDIC', ''):<12} "
            f"{row.get('CARDNO', ''):<16} "
            f"{row.get('APPRLIMT', 0):<18.2f} "
            f"{row.get('MONITOR', ''):<4} "
            f"{row.get('TYPE', ''):<4} "
            f"{row.get('ACCTNO', 0):>13} "
            f"{row.get('CURBAL', 0):>15,.2f}"
        )
        report_lines.append(row_line)
    
    report_lines.append("=" * 120)
    
    # Write report
    output_file = OUTPUT_PATH / f"UNDP_REPORT_{RDTEB}_{RDTEA}.txt"
    with open(output_file, 'w') as f:
        for line in report_lines:
            f.write(line + '\n')
    
    print(f"Report generated: {output_file}")

# Generate report
generate_report(final, RDTEB, RDTEA)

conn.close()
