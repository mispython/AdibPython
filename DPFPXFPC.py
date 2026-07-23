"""
EIMAR301 SAS to Python conversion
Multi-report system for HP Direct loan analysis with different criteria

CHANGES FROM PRIOR VERSION
---------------------------
1. REPTDATE.parquet control file removed. The reporting date is now derived
   at runtime from today's date, and the previous-month date is derived with
   plain datetime/timedelta arithmetic (first-of-month minus 1 day gives the
   last day of the previous month, from which PMTH/PYEAR/PREPTDTE are built).
2. LOANTEMP is now read from a SAS dataset (loantemp.sas7bdat) using
   pyreadstat instead of a parquet file.
3. Branch lookup (LKP_BRANCH) is now read from a flat (plain text) file
   instead of a parquet file. The reader below auto-detects a delimiter
   (pipe, comma, semicolon, tab) and falls back to whitespace-splitting.
   If LKP_BRANCH is actually fixed-width, set FIXED_WIDTH_SPECS below and
   the loader will use pandas.read_fwf instead - adjust column
   positions/names to match your real file layout.
4. All outputs are now written as plain delimited text files (.txt) instead
   of parquet, one per report, saved to OUTPUT_PATH.
"""

from pathlib import Path
from datetime import date, timedelta
import polars as pl
import pandas as pd
import pyreadstat
from typing import Dict, List

# ============================================================================
# 0. Paths / configuration
# ============================================================================

BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR301"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

LOANTEMP_PATH = INPUT_PATH / "BNM/LOANTEMP.sas7bdat"
LKP_BRANCH_PATH = INPUT_PATH / "LKP_BRANCH.txt"   # flat file - adjust extension/name as needed

# If LKP_BRANCH is fixed-width instead of delimited, fill this in and set
# USE_FIXED_WIDTH = True. colspecs are (start, end) 0-based, end-exclusive.
USE_FIXED_WIDTH = False
FIXED_WIDTH_SPECS = {
    "colspecs": [(0, 6), (7, 15)],
    "names": ["BRANCH", "BRHCODE"],
}

TEXT_COL_WIDTH = 15   # default column width used when writing .txt reports
TEXT_DELIM = "|"      # delimiter used between columns in .txt reports


# ============================================================================
# 1. REPTDATE Processing with Previous Month Calculation (no control file)
# ============================================================================

def process_repdate() -> Dict[str, object]:
    """
    Build the reporting-date variables using today's date instead of reading
    REPTDATE.parquet. Previous month is derived purely with date/timedelta
    arithmetic: take the 1st of the current month and subtract one day to
    land on the last day of the previous month, then read PMTH/PYEAR off
    that date.
    """
    repdate = date.today()

    first_of_this_month = repdate.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)

    pmth = last_day_prev_month.month
    pyear = last_day_prev_month.year
    pdate = date(pyear, pmth, 1)  # 1st day of previous month, same semantics as before

    return {
        'RDATE': repdate.strftime("%d%m%y"),   # DDMMYY8.
        'REPTYEAR': str(repdate.year),         # YEAR4.
        'REPTMON': f"{repdate.month:02d}",     # Z2.
        'REPTDAY': f"{repdate.day:02d}",       # Z2.
        'REPTDATE': repdate,
        'PREPTDTE': pdate,                     # Previous month date
        'PMTH': pmth,
        'PYEAR': pyear,
    }


# ============================================================================
# 2. Load and Preprocess Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """Load branch lookup data from a flat text file (LKP_BRANCH)."""
    if not LKP_BRANCH_PATH.exists():
        print(f"   WARNING: {LKP_BRANCH_PATH} not found - using empty branch lookup")
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

    if USE_FIXED_WIDTH:
        pdf = pd.read_fwf(
            LKP_BRANCH_PATH,
            colspecs=FIXED_WIDTH_SPECS["colspecs"],
            names=FIXED_WIDTH_SPECS["names"],
            header=None,
        )
        return pl.from_pandas(pdf).with_columns(
            pl.col("BRANCH").cast(pl.Int64, strict=False)
        )

    # Try common delimiters in order; fall back to whitespace splitting.
    for delim in ["|", ",", ";", "\t"]:
        try:
            df = pl.read_csv(LKP_BRANCH_PATH, separator=delim, has_header=True)
            if df.width >= 2:
                break
        except Exception:
            df = None
    else:
        df = None

    if df is None or df.width < 2:
        # Fallback: whitespace-delimited flat file with no header
        pdf = pd.read_csv(LKP_BRANCH_PATH, sep=r"\s+", header=None,
                           names=["BRANCH", "BRHCODE"])
        df = pl.from_pandas(pdf)

    # Normalize column names in case the source file uses different casing
    rename_map = {}
    for col in df.columns:
        upper = col.strip().upper()
        if upper in ("BRANCH", "BRANCH_NO", "BR_NO"):
            rename_map[col] = "BRANCH"
        elif upper in ("BRHCODE", "BR_CODE", "BRANCH_CODE"):
            rename_map[col] = "BRHCODE"
    if rename_map:
        df = df.rename(rename_map)

    df = df.with_columns(pl.col("BRANCH").cast(pl.Int64, strict=False))
    return df


def load_and_filter_loans(hpd_list: List[str], variables: Dict) -> pl.DataFrame:
    """Load and filter HP Direct loans with basic criteria."""

    # Load loan data from the SAS dataset via pyreadstat
    loan_pdf, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_PATH))
    loan_df = pl.from_pandas(loan_pdf)

    # Normalize column names to upper-case to match the rest of the script
    loan_df = loan_df.rename({c: c.upper() for c in loan_df.columns})

    # Convert HPD list to numbers
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]

    # Apply filters: BALANCE > 0 AND BORSTAT != 'Z' AND PRODUCT IN &HPD
    filtered = loan_df.filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(hpd_numbers))
    )

    # Load and merge branch data
    branch_df = load_branch_data()
    merged = filtered.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["BRANCH"])

    return merged


# ============================================================================
# 3. Text-file output helper
# ============================================================================

def write_text_report(df: pl.DataFrame, filepath: Path, title: str = "") -> None:
    """
    Write a Polars DataFrame out as a plain delimited text report.
    Columns are padded to TEXT_COL_WIDTH and separated by TEXT_DELIM,
    with a header row and a simple divider line - similar in spirit to a
    classic mainframe printed report.
    """
    lines = []
    if title:
        lines.append(title)
        lines.append("=" * max(len(title), 40))

    if df.is_empty():
        lines.append("(no records)")
        filepath.write_text("\n".join(lines) + "\n")
        return

    columns = df.columns
    header = TEXT_DELIM.join(f"{c:<{TEXT_COL_WIDTH}}" for c in columns)
    divider = "-" * len(header)
    lines.append(header)
    lines.append(divider)

    for row in df.iter_rows():
        line = TEXT_DELIM.join(f"{str(val):<{TEXT_COL_WIDTH}}" for val in row)
        lines.append(line)

    filepath.write_text("\n".join(lines) + "\n")


# ============================================================================
# 4. Report A: 2+ Months Arrears and 2 Installments or Less
# ============================================================================

def create_report_a_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report A: ARREAR2 >= 3 OR bad BORSTAT OR new loans with DAYDIFF >= 8"""

    # First condition: ARREAR2 >= 3 OR BORSTAT in R/I/F/Y
    condition1 = loan_df.filter(
        (pl.col("ARREAR2") >= 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
    )

    # Second condition: ISSDTE >= PREPTDTE AND DAYDIFF >= 8 (new loans)
    prept_date = variables['PREPTDTE']
    condition2 = loan_df.filter(
        (pl.col("ISSDTE") >= prept_date) &
        (pl.col("DAYDIFF") >= 8)
    )

    # Combine both conditions
    report_a_data = pl.concat([condition1, condition2], how="diagonal").unique()

    # Add categories and formatting
    categorized = report_a_data.with_columns([
        # Set ARREAR2 = 15 for BORSTAT = 'F'
        pl.when(pl.col("BORSTAT") == "F")
        .then(pl.lit(15))
        .otherwise(pl.col("ARREAR2"))
        .alias("ARREAR2_ADJ"),

        # Create ARREARS classification (simplified)
        pl.when(pl.col("ARREAR2") < 3)
        .then(pl.lit("< 3 MTHS"))
        .when(pl.col("ARREAR2") < 6)
        .then(pl.lit("3-5 MTHS"))
        .when(pl.col("ARREAR2") < 9)
        .then(pl.lit("6-8 MTHS"))
        .when(pl.col("ARREAR2") < 12)
        .then(pl.lit("9-11 MTHS"))
        .otherwise(pl.lit("12+ MTHS"))
        .alias("ARREARS"),

        # CACBR classification (simplified - would need external format)
        pl.when(pl.col("BRANCH") < 100)
        .then(pl.lit("000"))
        .otherwise(pl.col("BRANCH").cast(pl.Utf8))
        .alias("CACBR"),

        # Categories
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("A"))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("B"))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("C"))
        .otherwise(pl.lit("D"))
        .alias("CAT"),

        # Type descriptions
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("HP DIRECT(CONV) "))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("HP (380,381) "))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("AITAB "))
        .otherwise(pl.lit("OTHER"))
        .alias("TYPE")
    ])

    # Sort for report
    return categorized.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])


def generate_report_a_summary(report_a_df: pl.DataFrame, variables: Dict):
    """Generate Report A summary - Non-CAC branches only"""

    non_cac_data = report_a_df.filter(pl.col("CACBR") == "000")

    if len(non_cac_data) == 0:
        print("   No data for non-CAC branches in Report A")
        write_text_report(non_cac_data, OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
        return

    summary_data = []

    for cat in sorted(non_cac_data["CAT"].unique().to_list()):
        cat_data = non_cac_data.filter(pl.col("CAT") == cat)

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)

            branch_total = branch_data["BALANCE"].sum()
            branch_accounts = len(branch_data)

            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)

                summary_data.append({
                    "REPORT": "A",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": branch_data["BRHCODE"][0],
                    "ARREAR_BUCKET": arrear,
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total,
                    "ARREARS_CLASS": arrear_data["ARREARS"][0] if len(arrear_data) > 0 else ""
                })

            summary_data.append({
                "REPORT": "A_BRANCH_SUMMARY",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": branch_accounts,
                "TOTAL_BALANCE": branch_total,
                "ARREARS_CLASS": "BRANCH TOTAL"
            })

        cat_total = cat_data["BALANCE"].sum()
        cat_accounts = len(cat_data)
        summary_data.append({
            "REPORT": "A_CATEGORY_SUMMARY",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": cat_accounts,
            "TOTAL_BALANCE": cat_total,
            "ARREARS_CLASS": "CATEGORY TOTAL"
        })

    summary_df = pl.DataFrame(summary_data) if summary_data else pl.DataFrame()
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
    print(f"\u2713 Report A summary saved: {len(summary_df)} records")


# ============================================================================
# 5. Report B: 3-8 Months Arrears (Excluding BORSTAT F/I/R)
# ============================================================================

def create_report_b_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report B: ARREAR2 4-9 months, exclude BORSTAT F/I/R"""

    report_b_data = loan_df.filter(
        (pl.col("ARREAR2") >= 4) &
        (pl.col("ARREAR2") < 10) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )

    categorized = report_b_data.with_columns([
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("A"))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("B"))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("C"))
        .otherwise(pl.lit("D"))
        .alias("CAT"),

        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("HP DIRECT(CONV) "))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("HP (380,381) "))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("AITAB "))
        .otherwise(pl.lit("OTHER"))
        .alias("TYPE"),

        pl.when(pl.col("ARREAR2") < 6)
        .then(pl.lit("4-5 MTHS"))
        .when(pl.col("ARREAR2") < 8)
        .then(pl.lit("6-7 MTHS"))
        .otherwise(pl.lit("8-9 MTHS"))
        .alias("ARREARS_RANGE")
    ])

    return categorized.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])


def generate_report_b_summary(report_b_df: pl.DataFrame, variables: Dict):
    """Generate Report B summary"""

    if len(report_b_df) == 0:
        print("   No data for Report B")
        write_text_report(report_b_df, OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
        return

    summary_data = []

    for cat in sorted(report_b_df["CAT"].unique().to_list()):
        cat_data = report_b_df.filter(pl.col("CAT") == cat)

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)

            branch_total = branch_data["BALANCE"].sum()
            branch_accounts = len(branch_data)

            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)

                summary_data.append({
                    "REPORT": "B",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": branch_data["BRHCODE"][0],
                    "ARREAR_BUCKET": arrear,
                    "ARREARS_RANGE": arrear_data["ARREARS_RANGE"][0] if len(arrear_data) > 0 else "",
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total
                })

            summary_data.append({
                "REPORT": "B_BRANCH_SUMMARY",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "ARREAR_BUCKET": "ALL",
                "ARREARS_RANGE": "4-9 MTHS",
                "ACCOUNT_COUNT": branch_accounts,
                "TOTAL_BALANCE": branch_total
            })

        cat_total = cat_data["BALANCE"].sum()
        cat_accounts = len(cat_data)
        summary_data.append({
            "REPORT": "B_CATEGORY_SUMMARY",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ARREARS_RANGE": "4-9 MTHS",
            "ACCOUNT_COUNT": cat_accounts,
            "TOTAL_BALANCE": cat_total
        })

    summary_df = pl.DataFrame(summary_data) if summary_data else pl.DataFrame()
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
    print(f"\u2713 Report B summary saved: {len(summary_df)} records")


# ============================================================================
# 6. Report C: New Releases Summary (2 Installments or Less)
# ============================================================================

def create_report_c_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report C: New loans with DAYDIFF >= 8, payment summary"""

    prept_date = variables['PREPTDTE']
    new_loans = loan_df.filter(
        (pl.col("ISSDTE") >= prept_date) &
        (pl.col("DAYDIFF") >= 8)
    )

    report_c_data = new_loans.with_columns([
        pl.when(pl.col("NOISTLPD") < 1)
        .then(pl.lit("NO PAYMENT"))
        .when((pl.col("NOISTLPD") >= 1) & (pl.col("NOISTLPD") < 2))
        .then(pl.lit("PAID 1 ISTL"))
        .otherwise(pl.lit("PAID 2 ISTL"))
        .alias("PAYDESC")
    ])

    return report_c_data


def generate_report_c_summary(report_c_df: pl.DataFrame, variables: Dict):
    """Generate Report C summary by branch and payment type"""

    if len(report_c_df) == 0:
        print("   No data for Report C")
        write_text_report(report_c_df, OUTPUT_PATH / "REPORT_C_SUMMARY.txt", "REPORT C SUMMARY")
        return

    summary = report_c_df.group_by(["BRHCODE", "PAYDESC"]).agg([
        pl.count().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM")
    ])

    branch_totals = report_c_df.group_by("BRHCODE").agg([
        pl.count().alias("TOTAL_NOACCT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE")
    ])

    summary_data = []
    for row in summary.iter_rows(named=True):
        summary_data.append({
            "REPORT": "C",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": row["PAYDESC"],
            "NO_OF_AC": row["NOACCT"],
            "OS_BALANCE": row["BALANCE_SUM"]
        })

    for row in branch_totals.iter_rows(named=True):
        summary_data.append({
            "REPORT": "C_TOTAL",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": "TOTAL",
            "NO_OF_AC": row["TOTAL_NOACCT"],
            "OS_BALANCE": row["TOTAL_BALANCE"]
        })

    summary_df = pl.DataFrame(summary_data) if summary_data else pl.DataFrame()
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_C_SUMMARY.txt", "REPORT C SUMMARY")
    print(f"\u2713 Report C summary saved: {len(summary_df)} records")


# ============================================================================
# 7. Report D: Accounts with Exactly 2 Installments Paid
# ============================================================================

def create_report_d_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report D: Accounts with exactly 2 installments paid"""

    report_d_data = loan_df.filter(
        (pl.col("NOISTLPD") >= 2) &
        (pl.col("NOISTLPD") < 3) &
        (pl.col("DAYDIFF") >= 8)
    )

    report_d_data = report_d_data.with_columns(
        pl.lit("PAID 2 ISTL").alias("PAYDESC")
    )

    return report_d_data


def generate_report_d_summary(report_d_df: pl.DataFrame, variables: Dict):
    """Generate Report D summary by branch"""

    if len(report_d_df) == 0:
        print("   No data for Report D")
        write_text_report(report_d_df, OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
        return

    summary = report_d_df.group_by("BRHCODE").agg([
        pl.count().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM")
    ])

    grand_total = report_d_df.select([
        pl.count().alias("TOTAL_NOACCT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE")
    ])

    summary_data = []
    for row in summary.iter_rows(named=True):
        summary_data.append({
            "REPORT": "D",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": "PAID 2 ISTL",
            "NO_OF_AC": row["NOACCT"],
            "OS_BALANCE": row["BALANCE_SUM"]
        })

    for row in grand_total.iter_rows(named=True):
        summary_data.append({
            "REPORT": "D_TOTAL",
            "BRHCODE": "TOTAL",
            "PAYDESC": "PAID 2 ISTL",
            "NO_OF_AC": row["TOTAL_NOACCT"],
            "OS_BALANCE": row["TOTAL_BALANCE"]
        })

    summary_df = pl.DataFrame(summary_data) if summary_data else pl.DataFrame()
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
    print(f"\u2713 Report D summary saved: {len(summary_df)} records")


# ============================================================================
# 8. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMAR301 SAS to Python Conversion - Multi-Report System")
    print("=" * 60)

    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]

    # 1. Process REPTDATE with previous month calculation (no control file)
    print("\n1. Processing REPTDATE with previous month (datetime/timedelta)...")
    variables = process_repdate()
    print(f"   Current Date: {variables['RDATE']}")
    print(f"   Previous Month Date: {variables['PREPTDTE']}")

    # 2. Load and filter loans
    print("\n2. Loading and filtering HP Direct loans...")
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
    print(f"   Filtered HP Direct loans: {len(filtered_loans)}")

    # 3. Generate Report A
    print("\n3. Generating Report A (EIMAR301-A)...")
    report_a_data = create_report_a_data(filtered_loans, variables)
    print(f"   Report A accounts: {len(report_a_data)}")
    generate_report_a_summary(report_a_data, variables)

    # 4. Generate Report B
    print("\n4. Generating Report B (EIMAR301-B)...")
    report_b_data = create_report_b_data(filtered_loans, variables)
    print(f"   Report B accounts: {len(report_b_data)}")
    generate_report_b_summary(report_b_data, variables)

    # 5. Generate Report C
    print("\n5. Generating Report C (EIMAR301-C)...")
    report_c_data = create_report_c_data(filtered_loans, variables)
    print(f"   Report C accounts: {len(report_c_data)}")
    generate_report_c_summary(report_c_data, variables)

    # 6. Generate Report D
    print("\n6. Generating Report D (EIMAR301-D)...")
    report_d_data = create_report_d_data(filtered_loans, variables)
    print(f"   Report D accounts: {len(report_d_data)}")
    generate_report_d_summary(report_d_data, variables)

    # 7. Create combined analysis
    print("\n7. Creating combined analysis...")

    overall_stats = {
        "TOTAL_HP_LOANS": len(filtered_loans),
        "REPORT_A_ACCOUNTS": len(report_a_data),
        "REPORT_B_ACCOUNTS": len(report_b_data),
        "REPORT_C_ACCOUNTS": len(report_c_data),
        "REPORT_D_ACCOUNTS": len(report_d_data),
        "TOTAL_BALANCE": filtered_loans["BALANCE"].sum(),
        "AVG_BALANCE": filtered_loans["BALANCE"].mean(),
        "AVG_ARREAR": filtered_loans["ARREAR2"].mean(),
        "NEW_LOANS_COUNT": len(filtered_loans.filter(pl.col("ISSDTE") >= variables['PREPTDTE'])),
        "NPL_COUNT": len(filtered_loans.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))
    }

    overall_df = pl.DataFrame([overall_stats])
    write_text_report(overall_df, OUTPUT_PATH / "OVERALL_STATISTICS.txt", "OVERALL STATISTICS")

    # 8. Create detailed data extracts (as text files)
    print("\n8. Creating detailed data extracts...")
    write_text_report(report_a_data, OUTPUT_PATH / "REPORT_A_DETAILED.txt", "REPORT A DETAILED")
    write_text_report(report_b_data, OUTPUT_PATH / "REPORT_B_DETAILED.txt", "REPORT B DETAILED")
    write_text_report(report_c_data, OUTPUT_PATH / "REPORT_C_DETAILED.txt", "REPORT C DETAILED")
    write_text_report(report_d_data, OUTPUT_PATH / "REPORT_D_DETAILED.txt", "REPORT D DETAILED")

    # 9. Create branch performance analysis
    print("\n9. Creating branch performance analysis...")

    branch_metrics = []
    for branch in filtered_loans["BRANCH"].unique().to_list():
        branch_data = filtered_loans.filter(pl.col("BRANCH") == branch)

        npl_count = len(branch_data.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))

        new_loans = len(branch_data.filter(pl.col("ISSDTE") >= variables['PREPTDTE']))
        avg_arrear = branch_data["ARREAR2"].mean()
        paid_2_or_less = len(branch_data.filter(pl.col("NOISTLPD") <= 2))

        branch_metrics.append({
            "BRANCH": branch,
            "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
            "TOTAL_ACCOUNTS": len(branch_data),
            "TOTAL_BALANCE": branch_data["BALANCE"].sum(),
            "NPL_COUNT": npl_count,
            "NPL_PERCENTAGE": (npl_count / len(branch_data) * 100) if len(branch_data) > 0 else 0,
            "NEW_LOANS_COUNT": new_loans,
            "AVG_ARREAR": avg_arrear,
            "PAID_2_OR_LESS": paid_2_or_less,
            "PAYMENT_RATIO": (paid_2_or_less / len(branch_data) * 100) if len(branch_data) > 0 else 0
        })

    branch_df = pl.DataFrame(branch_metrics) if branch_metrics else pl.DataFrame()
    write_text_report(branch_df, OUTPUT_PATH / "BRANCH_PERFORMANCE.txt", "BRANCH PERFORMANCE")
    print(f"\u2713 Branch performance analysis saved: {len(branch_df)} branches")

    # 10. Save variables
    variables_df = pl.DataFrame([variables])
    write_text_report(variables_df, OUTPUT_PATH / "EIMAR301_VARIABLES.txt", "EIMAR301 VARIABLES")

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total HP Direct loans processed: {len(filtered_loans)}")
    print(f"Report A (2+ months arrears): {len(report_a_data)}")
    print(f"Report B (3-8 months arrears): {len(report_b_data)}")
    print(f"Report C (New releases): {len(report_c_data)}")
    print(f"Report D (2 installments paid): {len(report_d_data)}")
    print(f"Previous month date: {variables['PREPTDTE']}")
    print(f"Output saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
