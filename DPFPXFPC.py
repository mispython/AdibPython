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

LOGIC FIXES (match SAS original behaviour)
------------------------------------------
A. Report A / Report B CAT assignment
   In SAS, the DATA step emits MULTIPLE OUTPUT rows when a PRODUCT matches
   more than one of the IF ... OUTPUT blocks (there is no ELSE). For
   example PRODUCT 380/381 emits BOTH a CAT='A' row AND a CAT='B' row.
   The previous polars `when/then` chain assigned only the FIRST matching
   category per row, dropping the duplicate-category rows. Now we
   replicate the SAS behaviour by explicitly building one row per
   (record, matching category).

B. Report A ARREAR2_ADJ = 15 for BORSTAT='F'
   In SAS, `IF BORSTAT = 'F' THEN ARREAR2 = 15;` occurs BEFORE the PUT for
   ARREARS and BEFORE sorting, so the override affects BOTH the sort order
   and the ARREARS label. In the prior Python version, ARREARS was computed
   from the ORIGINAL ARREAR2 (so a BORSTAT='F' row would still show the
   pre-override arrears class). Now ARREARS is computed from ARREAR2_ADJ,
   and the final sort uses ARREAR2_ADJ.

C. Report A/B sorting
   SAS sorts BY CAT BRANCH ARREAR2 DESCENDING BALANCE. We now sort on
   ARREAR2_ADJ (post-override) in Report A, and on raw ARREAR2 in Report B,
   then BRANCH, then BALANCE descending.

D. Report A/B summary must use ARREAR2_ADJ (Report A) / ARREAR2 (Report B)
   for the arrear bucket key, matching the BY-variable used in the SAS
   subtotal groups. Previously Report A summarised on raw ARREAR2, so
   BORSTAT='F' rows were bucketed under the original value.

E. Report A `FILE PRINT` filter
   The SAS Report A listing filters `WHERE CACBR = '000'`. That is a
   program-specific CACBR FORMAT, not a simple branch<100 rule. We now
   read CACBR (and ARREARS) from CSV lookup tables shipped alongside the
   report (CACBRCH.csv, ARRCLASS.csv) if present, and fall back to the
   simple built-in defaults otherwise. Edit the fallback helpers below
   if your real formats differ.

F. Report B excludes BORSTAT F/I/R via WHERE, but Report A's LOAN1
   dataset is still CAT-assigned the same way. No change required beyond
   keeping ARREAR2_ADJ for the Report A path only.
"""

from pathlib import Path
from datetime import date, timedelta
import re
from typing import Dict, List, Optional

import polars as pl
import pandas as pd
import pyreadstat

# ============================================================================
# 0. Paths / configuration
# ============================================================================

LOANTEMP_PATH = Path("/dwh/dwh_m/tempsource/loantemp.sas7bdat")
LKP_BRANCH_PATH = Path("/sasdata/rawdata/lookup/LKP_BRANCH")
OUTPUT_PATH = Path("/host_pq/mis/output/report/EIMIR")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Optional format lookup files (CSV, two columns: KEY,VALUE). If present
# and non-empty, they override the built-in fallback classification.
ARRCLASS_CSV = Path("/sasdata/rawdata/lookup/ARRCLASS.csv")   # e.g. 0,"< 3 MTHS" ...
CACBRCH_CSV = Path("/sasdata/rawdata/lookup/CACBRCH.csv")     # e.g. BRANCH -> CACBR

# If LKP_BRANCH is fixed-width instead of delimited, fill this in and set
# USE_FIXED_WIDTH = True. colspecs are (start, end) 0-based, end-exclusive.
USE_FIXED_WIDTH = False
FIXED_WIDTH_SPECS = {
    "colspecs": [(0, 6), (7, 15)],
    "names": ["BRANCH", "BRHCODE"],
}

TEXT_COL_WIDTH = 15   # default column width used when writing .txt reports
TEXT_DELIM = "|"      # delimiter used between columns in .txt reports

# Product -> category map, mirroring the SAS IF ... OUTPUT blocks.
# In SAS, PRODUCTS that match more than one IF (e.g. 380, 381) produce
# MULTIPLE rows, one per matching category. We keep this mapping as a list
# of (CAT, TYPE, product-set) rules that are ALL evaluated per record.
PRODUCT_CATEGORY_RULES = [
    ("A", "HP DIRECT(CONV) ", [380, 381, 700, 705, 720, 725]),
    ("B", "HP (380,381)    ", [380, 381]),
    ("C", "AITAB           ", [128, 130, 131, 132]),
]


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
    pdate = date(pyear, pmth, 1)

    return {
        'RDATE': repdate.strftime("%d/%m/%y"),
        'REPTYEAR': str(repdate.year),
        'REPTMON': f"{repdate.month:02d}",
        'REPTDAY': f"{repdate.day:02d}",
        'REPTDATE': repdate,
        'PREPTDTE': pdate,
        'PMTH': pmth,
        'PYEAR': pyear,
    }


# ============================================================================
# 2. Format lookups (ARRCLASS, CACBRCH)
# ============================================================================

def _load_lookup_csv(path: Path) -> Optional[Dict[str, str]]:
    """Load a simple KEY,VALUE CSV lookup; return None if not present/empty."""
    if not path.exists():
        return None
    try:
        rows = {}
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip().strip('"').strip("'") for p in line.split(",")]
                if len(parts) < 2:
                    continue
                rows[parts[0]] = parts[1]
        return rows if rows else None
    except Exception as e:
        print(f"   WARNING: could not read {path}: {e}")
        return None


def build_arrclass_lookup() -> Dict[str, str]:
    """
    SAS: PUT(ARREAR2, ARRCLASS.)
    Returns a mapping from ARREAR2-as-int-string -> label string.

    Priority: ARRCLASS.csv if present, else built-in fallback that matches
    the production label style seen in the sample output
    ('2 - < 3 MTH', '3 - < 6 MTH', etc.).
    """
    csv = _load_lookup_csv(ARRCLASS_CSV)
    if csv:
        return csv

    fallback = {}
    for a in range(0, 20):
        if a < 3:
            fallback[str(a)] = "2 - < 3 MTH"
        elif a < 6:
            fallback[str(a)] = "3 - < 6 MTH"
        elif a < 9:
            fallback[str(a)] = "6 - < 9 MTH"
        elif a < 12:
            fallback[str(a)] = "9 - < 12 MTH"
        else:
            fallback[str(a)] = "12 MTH & ABOVE"
    return fallback


def build_cacbr_lookup() -> Dict[str, str]:
    """
    SAS: CACBR = PUT(BRANCH, CACBRCH.)
    Returns a mapping from BRANCH-as-int-string -> CACBR string.
    """
    csv = _load_lookup_csv(CACBRCH_CSV)
    if csv:
        return csv
    # Fallback (approximation: non-CAC = '000'). Only used if no CSV is present.
    fallback = {}
    for b in range(1, 100):
        fallback[str(b)] = "000"
    return fallback


# ============================================================================
# 3. Load and Preprocess Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """
    Load branch lookup data from a flat text file (LKP_BRANCH).
    Same parsing strategy as before: fixed-width -> delimited -> whitespace.
    """
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

    df = None
    for delim in ["|", ",", ";", "\t"]:
        try:
            candidate = pl.read_csv(LKP_BRANCH_PATH, separator=delim, has_header=True)
            if candidate.width >= 2:
                df = candidate
                break
        except Exception:
            continue

    if df is None:
        rows = []
        raw_lines_sample = []
        with open(LKP_BRANCH_PATH, "r", encoding="utf-8", errors="replace") as f:
            for i, raw_line in enumerate(f):
                if i < 8:
                    raw_lines_sample.append(raw_line.rstrip("\n"))
                line = raw_line.strip()
                if not line:
                    continue
                tokens = line.split()
                if len(tokens) < 2:
                    continue
                branch_raw = tokens[0]
                brhcode = tokens[1]
                digits = re.sub(r"\D", "", branch_raw)
                if digits == "":
                    continue
                rows.append({"BRANCH": int(digits), "BRHCODE": brhcode})

        print("   [debug] LKP_BRANCH first 8 raw lines (repr):")
        for rl in raw_lines_sample:
            print(f"   [debug]   {rl!r}")
        print(f"   [debug] first 8 parsed rows: {rows[:8]}")

        df = pl.DataFrame(rows, schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

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

    loan_pdf, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_PATH))
    loan_df = pl.from_pandas(loan_pdf)
    loan_df = loan_df.rename({c: c.upper() for c in loan_df.columns})
    loan_df = loan_df.with_columns(pl.col("BRANCH").cast(pl.Int64, strict=False))

    print(f"   [debug] raw LOANTEMP rows: {len(loan_df)}")
    print(f"   [debug] BALANCE>0 rows: {len(loan_df.filter(pl.col('BALANCE') > 0))}")
    print(f"   [debug] BORSTAT != 'Z' rows: {len(loan_df.filter(pl.col('BORSTAT') != 'Z'))}")
    print(f"   [debug] sample PRODUCT values: {loan_df['PRODUCT'].unique().to_list()[:15]}")
    print(f"   [debug] sample BORSTAT values: {loan_df['BORSTAT'].unique().to_list()[:15]}")

    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    print(f"   [debug] HPD filter numbers: {hpd_numbers}")
    print(f"   [debug] PRODUCT in HPD rows: {len(loan_df.filter(pl.col('PRODUCT').is_in(hpd_numbers)))}")

    filtered = loan_df.filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(hpd_numbers))
    )
    print(f"   [debug] rows after all three filters (pre-join): {len(filtered)}")

    branch_df = load_branch_data()
    print(f"   [debug] LKP_BRANCH rows loaded: {len(branch_df)}")
    print(f"   [debug] sample branch_df BRANCH values: {branch_df['BRANCH'].unique().to_list()[:15]}")
    print(f"   [debug] sample loan BRANCH values: {filtered['BRANCH'].unique().to_list()[:15]}")

    merged = filtered.join(branch_df, on="BRANCH", how="inner").sort(["BRANCH"])
    print(f"   [debug] rows after branch join: {len(merged)}")

    return merged


# ============================================================================
# 4. Text-file output helper
# ============================================================================

def write_text_report(df: pl.DataFrame, filepath: Path, title: str = "") -> None:
    """Write a Polars DataFrame out as a plain delimited text report."""
    lines = []
    if title:
        lines.append(title)
        lines.append("=" * max(len(title), 40))

    if df is None or df.is_empty():
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
# 5. Category explosion helper (mirrors SAS "IF ... THEN OUTPUT" pattern)
# ============================================================================

def explode_categories(df: pl.DataFrame) -> pl.DataFrame:
    """
    In SAS, LOAN1 is built by multiple independent IF PRODUCT IN (...) THEN
    DO; CAT='A'; OUTPUT; END; blocks. A record whose PRODUCT matches more
    than one block is emitted MULTIPLE times, once per matching category.
    This function produces exactly that: one output row per
    (input row, matching category rule).
    """
    if df.is_empty():
        return df

    parts = []
    for cat, type_label, prods in PRODUCT_CATEGORY_RULES:
        sub = df.filter(pl.col("PRODUCT").is_in(prods))
        if sub.is_empty():
            continue
        sub = sub.with_columns([
            pl.lit(cat).alias("CAT"),
            pl.lit(type_label).alias("TYPE"),
        ])
        parts.append(sub)

    if not parts:
        # No matches at all - return empty with CAT/TYPE columns.
        return df.head(0).with_columns([
            pl.lit(None).cast(pl.Utf8).alias("CAT"),
            pl.lit(None).cast(pl.Utf8).alias("TYPE"),
        ])

    return pl.concat(parts, how="diagonal")


# ============================================================================
# 6. Report A: 2+ Months Arrears and 2 Installments or Less
# ============================================================================

def create_report_a_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    Mirrors SAS DATA LOAN / DATA LOAN1 for Report A.

    SAS:
      DATA LOAN; SET LNTEMP;
        IF ARREAR2 GE 3 OR BORSTAT IN ('R','I','F','Y') THEN OUTPUT;
        IF ISSDTE GE &PREPTDTE AND DAYDIFF >= 8 THEN OUTPUT;
      RUN;
    Both IFs may OUTPUT the same record -> duplicates are possible in SAS.
    We keep them distinct (no unique()).
    """
    cond1 = loan_df.filter(
        (pl.col("ARREAR2") >= 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
    )
    cond2 = loan_df.filter(
        (pl.col("ISSDTE") >= variables['PREPTDTE']) &
        (pl.col("DAYDIFF") >= 8)
    )
    loan = pl.concat([cond1, cond2], how="diagonal")

    # SAS: IF BORSTAT = 'F' THEN ARREAR2 = 15;  (overrides ARREAR2 BEFORE
    # PUT ARREARS / sorting / subtotals)
    loan = loan.with_columns(
        pl.when(pl.col("BORSTAT") == "F")
        .then(pl.lit(15))
        .otherwise(pl.col("ARREAR2"))
        .alias("ARREAR2_ADJ")
    )

    # SAS: ARREARS = PUT(ARREAR2, ARRCLASS.)  where ARREAR2 is now the ADJ
    # value; and CACBR = PUT(BRANCH, CACBRCH.)
    arrclass = build_arrclass_lookup()
    cacbrch = build_cacbr_lookup()

    def _arr(a):
        try:
            return arrclass.get(str(int(a)), "")
        except (TypeError, ValueError):
            return ""

    def _cac(b):
        try:
            return cacbrch.get(str(int(b)), "000")
        except (TypeError, ValueError):
            return "000"

    loan = loan.with_columns([
        pl.col("ARREAR2_ADJ").map_elements(_arr, return_dtype=pl.Utf8).alias("ARREARS"),
        pl.col("BRANCH").map_elements(_cac, return_dtype=pl.Utf8).alias("CACBR"),
    ])

    # SAS: multiple IF PRODUCT IN (...) THEN DO CAT=..; OUTPUT; END;
    #     -> emit one row per matching category.
    categorized = explode_categories(loan)

    if categorized.is_empty():
        return categorized

    # SAS: PROC SORT BY CAT BRANCH ARREAR2 DESCENDING BALANCE;
    # ARREAR2 here is the ADJ value.
    return categorized.sort(
        ["CAT", "BRANCH", "ARREAR2_ADJ", "BALANCE"],
        descending=[False, False, False, True],
    )


def generate_report_a_summary(report_a_df: pl.DataFrame, variables: Dict):
    """
    Mirrors SAS Report A listing: WHERE CACBR = '000'; grouped BY
    CAT BRANCH ARREAR2 (ADJ) DESCENDING BALANCE, with SUBTOTAL per arrear,
    BRANCH TOTAL per branch, GRAND TOTAL per CAT.
    """
    non_cac_data = report_a_df.filter(pl.col("CACBR") == "000")

    if non_cac_data.is_empty():
        print("   No data for non-CAC branches in Report A")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
        return

    summary_data = []
    grand_total_bal = 0.0
    grand_total_ac = 0

    for cat in sorted(non_cac_data["CAT"].unique().to_list()):
        cat_data = non_cac_data.filter(pl.col("CAT") == cat)
        cat_total_bal = 0.0
        cat_total_ac = 0

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            brhcode = branch_data["BRHCODE"][0] if len(branch_data) > 0 else ""

            branch_total_bal = 0.0
            branch_total_ac = 0

            # Bucket by ARREAR2_ADJ (the post-override BY-variable)
            for arrear in sorted(branch_data["ARREAR2_ADJ"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2_ADJ") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)

                summary_data.append({
                    "REPORT": "A_SUBTOTAL",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": brhcode,
                    "ARREAR_BUCKET": arrear,
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total,
                    "ARREARS_CLASS": arrear_data["ARREARS"][0] if len(arrear_data) > 0 else "",
                })
                branch_total_bal += arrear_total
                branch_total_ac += arrear_accounts

            summary_data.append({
                "REPORT": "A_BRANCH_TOTAL",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": brhcode,
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": branch_total_ac,
                "TOTAL_BALANCE": branch_total_bal,
                "ARREARS_CLASS": "BRANCH TOTAL",
            })
            cat_total_bal += branch_total_bal
            cat_total_ac += branch_total_ac

        summary_data.append({
            "REPORT": "A_GRAND_TOTAL",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": cat_total_ac,
            "TOTAL_BALANCE": cat_total_bal,
            "ARREARS_CLASS": "GRAND TOTAL",
        })
        grand_total_bal += cat_total_bal
        grand_total_ac += cat_total_ac

    summary_data.append({
        "REPORT": "A_OVERALL_GRAND",
        "CATEGORY": "ALL",
        "BRANCH": "ALL",
        "BRHCODE": "",
        "ARREAR_BUCKET": "ALL",
        "ACCOUNT_COUNT": grand_total_ac,
        "TOTAL_BALANCE": grand_total_bal,
        "ARREARS_CLASS": "OVERALL TOTAL",
    })

    summary_df = pl.DataFrame(summary_data)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
    print(f"\u2713 Report A summary saved: {len(summary_df)} records")


# ============================================================================
# 7. Report B: 3-8 Months Arrears (Excluding BORSTAT F/I/R)
# ============================================================================

def create_report_b_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    Mirrors SAS Report B WHERE clause:
        (ARREAR2 >= 4 AND ARREAR2 < 10) AND BORSTAT NOT IN ('F','I','R')
    and reuses the SAME CAT/TYPE explosion as Report A (in SAS, Report B
    reads LOAN1, which had the multi-OUTPUT explosion applied).
    """
    report_b_data = loan_df.filter(
        (pl.col("ARREAR2") >= 4) &
        (pl.col("ARREAR2") < 10) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )

    # Reuse same category rules (Report B in SAS reads LOAN1 which already
    # contains CAT/TYPE produced by the same IF ... OUTPUT blocks).
    categorized = explode_categories(report_b_data)

    if categorized.is_empty():
        return categorized

    return categorized.sort(
        ["CAT", "BRANCH", "ARREAR2", "BALANCE"],
        descending=[False, False, False, True],
    )


def generate_report_b_summary(report_b_df: pl.DataFrame, variables: Dict):
    """Mirrors SAS Report B listing subtotals BY CAT BRANCH ARREAR2."""
    if report_b_df.is_empty():
        print("   No data for Report B")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
        return

    summary_data = []
    grand_total_bal = 0.0
    grand_total_ac = 0

    for cat in sorted(report_b_df["CAT"].unique().to_list()):
        cat_data = report_b_df.filter(pl.col("CAT") == cat)
        cat_total_bal = 0.0
        cat_total_ac = 0

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            brhcode = branch_data["BRHCODE"][0] if len(branch_data) > 0 else ""

            branch_total_bal = 0.0
            branch_total_ac = 0

            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)

                summary_data.append({
                    "REPORT": "B_SUBTOTAL",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": brhcode,
                    "ARREAR_BUCKET": arrear,
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total,
                })
                branch_total_bal += arrear_total
                branch_total_ac += arrear_accounts

            summary_data.append({
                "REPORT": "B_BRANCH_TOTAL",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": brhcode,
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": branch_total_ac,
                "TOTAL_BALANCE": branch_total_bal,
            })
            cat_total_bal += branch_total_bal
            cat_total_ac += branch_total_ac

        summary_data.append({
            "REPORT": "B_GRAND_TOTAL",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": cat_total_ac,
            "TOTAL_BALANCE": cat_total_bal,
        })
        grand_total_bal += cat_total_bal
        grand_total_ac += cat_total_ac

    summary_data.append({
        "REPORT": "B_OVERALL_GRAND",
        "CATEGORY": "ALL",
        "BRANCH": "ALL",
        "BRHCODE": "",
        "ARREAR_BUCKET": "ALL",
        "ACCOUNT_COUNT": grand_total_ac,
        "TOTAL_BALANCE": grand_total_bal,
    })

    summary_df = pl.DataFrame(summary_data)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
    print(f"\u2713 Report B summary saved: {len(summary_df)} records")


# ============================================================================
# 8. Report C: New Releases Summary (2 Installments or Less)
# ============================================================================

def create_report_c_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    SAS NEWREL: SET LNTEMP; IF ISSDTE >= &PREPTDTE AND DAYDIFF >= 8;
    then PAYDESC = NO PAYMENT / PAID 1 ISTL / PAID 2 ISTL.
    """
    new_loans = loan_df.filter(
        (pl.col("ISSDTE") >= variables['PREPTDTE']) &
        (pl.col("DAYDIFF") >= 8)
    )

    return new_loans.with_columns(
        pl.when(pl.col("NOISTLPD") < 1)
        .then(pl.lit("NO PAYMENT"))
        .when((pl.col("NOISTLPD") >= 1) & (pl.col("NOISTLPD") < 2))
        .then(pl.lit("PAID 1 ISTL"))
        .otherwise(pl.lit("PAID 2 ISTL"))
        .alias("PAYDESC")
    )


def generate_report_c_summary(report_c_df: pl.DataFrame, variables: Dict):
    """
    Mirrors SAS PROC SUMMARY CLASS BRHCODE PAYDESC + PROC TABULATE.
    Layout: BRHCODE rows x PAYDESC columns (NO OF A/C, O/S BALANCE).
    """
    if report_c_df.is_empty():
        print("   No data for Report C")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_C_SUMMARY.txt", "REPORT C SUMMARY")
        return

    paydescs = ["NO PAYMENT", "PAID 1 ISTL", "PAID 2 ISTL"]
    summary = report_c_df.group_by(["BRHCODE", "PAYDESC"]).agg([
        pl.len().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM"),
    ])

    rows = []
    for brhcode in sorted(summary["BRHCODE"].unique().to_list()):
        bh = summary.filter(pl.col("BRHCODE") == brhcode)
        row = {"BRHCODE": brhcode}
        total_ac = 0
        total_bal = 0.0
        for pd_ in paydescs:
            sub = bh.filter(pl.col("PAYDESC") == pd_)
            ac = int(sub["NOACCT"].sum()) if len(sub) > 0 else 0
            bal = float(sub["BALANCE_SUM"].sum()) if len(sub) > 0 else 0.0
            row[f"{pd_} :: NO_OF_AC"] = ac
            row[f"{pd_} :: O_S_BALANCE"] = bal
            total_ac += ac
            total_bal += bal
        row["TOTAL :: NO_OF_AC"] = total_ac
        row["TOTAL :: O_S_BALANCE"] = total_bal
        rows.append(row)

    grand_ac = 0
    grand_bal = 0.0
    for r in rows:
        grand_ac += r["TOTAL :: NO_OF_AC"]
        grand_bal += r["TOTAL :: O_S_BALANCE"]
    rows.append({
        "BRHCODE": "TOTAL",
        **{k: sum(r[k] for r in rows) for k in rows[0] if k != "BRHCODE"},
    })

    summary_df = pl.DataFrame(rows)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_C_SUMMARY.txt", "REPORT C SUMMARY")
    print(f"\u2713 Report C summary saved: {len(summary_df)} records")


# ============================================================================
# 9. Report D: Accounts with Exactly 2 Installments Paid
# ============================================================================

def create_report_d_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    SAS ACCARR: SET LNTEMP; IF (2<=NOISTLPD<3) AND DAYDIFF>=8;
    PAYDESC='PAID 2 ISTL'.
    """
    report_d_data = loan_df.filter(
        (pl.col("NOISTLPD") >= 2) &
        (pl.col("NOISTLPD") < 3) &
        (pl.col("DAYDIFF") >= 8)
    )
    return report_d_data.with_columns(pl.lit("PAID 2 ISTL").alias("PAYDESC"))


def generate_report_d_summary(report_d_df: pl.DataFrame, variables: Dict):
    """Mirrors SAS PROC TABULATE for Report D (PAYDESC only) + ALL total."""
    if report_d_df.is_empty():
        print("   No data for Report D")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
        return

    summary = report_d_df.group_by("BRHCODE").agg([
        pl.len().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM"),
    ]).sort("BRHCODE")

    rows = []
    total_ac = 0
    total_bal = 0.0
    for row in summary.iter_rows(named=True):
        rows.append({
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": "PAID 2 ISTL",
            "NO_OF_AC": int(row["NOACCT"]),
            "O_S_BALANCE": float(row["BALANCE_SUM"]),
        })
        total_ac += int(row["NOACCT"])
        total_bal += float(row["BALANCE_SUM"])

    rows.append({
        "BRHCODE": "TOTAL",
        "PAYDESC": "PAID 2 ISTL",
        "NO_OF_AC": total_ac,
        "O_S_BALANCE": total_bal,
    })

    summary_df = pl.DataFrame(rows)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
    print(f"\u2713 Report D summary saved: {len(summary_df)} records")


# ============================================================================
# 10. Main Execution
# ============================================================================

def main():
    print("=" * 60)
    print("EIMAR301 SAS to Python Conversion - Multi-Report System")
    print("=" * 60)

    HPD_LIST = ["110", "115", "700", "705"]

    # 1. Reporting date variables
    print("\n1. Processing REPTDATE with previous month (datetime/timedelta)...")
    variables = process_repdate()
    print(f"   Current Date: {variables['RDATE']}")
    print(f"   Previous Month Date: {variables['PREPTDTE']}")

    # 2. Load and filter loans
    print("\n2. Loading and filtering HP Direct loans...")
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
    print(f"   Filtered HP Direct loans: {len(filtered_loans)}")

    # 3. Report A
    print("\n3. Generating Report A (EIMAR301-A)...")
    report_a_data = create_report_a_data(filtered_loans, variables)
    print(f"   Report A accounts: {len(report_a_data)}")
    generate_report_a_summary(report_a_data, variables)

    # 4. Report B
    print("\n4. Generating Report B (EIMAR301-B)...")
    report_b_data = create_report_b_data(filtered_loans, variables)
    print(f"   Report B accounts: {len(report_b_data)}")
    generate_report_b_summary(report_b_data, variables)

    # 5. Report C
    print("\n5. Generating Report C (EIMAR301-C)...")
    report_c_data = create_report_c_data(filtered_loans, variables)
    print(f"   Report C accounts: {len(report_c_data)}")
    generate_report_c_summary(report_c_data, variables)

    # 6. Report D
    print("\n6. Generating Report D (EIMAR301-D)...")
    report_d_data = create_report_d_data(filtered_loans, variables)
    print(f"   Report D accounts: {len(report_d_data)}")
    generate_report_d_summary(report_d_data, variables)

    # 7. Combined analysis
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
        )),
    }
    write_text_report(pl.DataFrame([overall_stats]),
                      OUTPUT_PATH / "OVERALL_STATISTICS.txt",
                      "OVERALL STATISTICS")

    # 8. Detailed extracts
    print("\n8. Creating detailed data extracts...")
    write_text_report(report_a_data, OUTPUT_PATH / "REPORT_A_DETAILED.txt", "REPORT A DETAILED")
    write_text_report(report_b_data, OUTPUT_PATH / "REPORT_B_DETAILED.txt", "REPORT B DETAILED")
    write_text_report(report_c_data, OUTPUT_PATH / "REPORT_C_DETAILED.txt", "REPORT C DETAILED")
    write_text_report(report_d_data, OUTPUT_PATH / "REPORT_D_DETAILED.txt", "REPORT D DETAILED")

    # 9. Branch performance
    print("\n9. Creating branch performance analysis...")
    branch_metrics = []
    for branch in filtered_loans["BRANCH"].unique().to_list():
        bd = filtered_loans.filter(pl.col("BRANCH") == branch)
        npl_count = len(bd.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))
        new_loans = len(bd.filter(pl.col("ISSDTE") >= variables['PREPTDTE']))
        paid_2_or_less = len(bd.filter(pl.col("NOISTLPD") <= 2))

        branch_metrics.append({
            "BRANCH": branch,
            "BRHCODE": bd["BRHCODE"][0] if len(bd) > 0 else "",
            "TOTAL_ACCOUNTS": len(bd),
            "TOTAL_BALANCE": bd["BALANCE"].sum(),
            "NPL_COUNT": npl_count,
            "NPL_PERCENTAGE": (npl_count / len(bd) * 100) if len(bd) > 0 else 0,
            "NEW_LOANS_COUNT": new_loans,
            "AVG_ARREAR": bd["ARREAR2"].mean(),
            "PAID_2_OR_LESS": paid_2_or_less,
            "PAYMENT_RATIO": (paid_2_or_less / len(bd) * 100) if len(bd) > 0 else 0,
        })

    branch_df = pl.DataFrame(branch_metrics) if branch_metrics else pl.DataFrame()
    write_text_report(branch_df, OUTPUT_PATH / "BRANCH_PERFORMANCE.txt", "BRANCH PERFORMANCE")
    print(f"\u2713 Branch performance analysis saved: {len(branch_df)} branches")

    # 10. Variables
    write_text_report(pl.DataFrame([variables]),
                      OUTPUT_PATH / "EIMAR301_VARIABLES.txt",
                      "EIMAR301 VARIABLES")

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
