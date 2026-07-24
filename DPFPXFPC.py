"""
EIMAR301 / EIMIR301 SAS to Python conversion
Multi-report system for HP Direct / AITAB loan arrears analysis

This version is rewritten directly against the original SAS source the user
supplied. Key behavioral corrections vs. the earlier draft are called out
in comments marked "CORRECTED:".

TWO EXTERNAL SAS FORMATS ARE NOT AVAILABLE TO THIS SCRIPT
----------------------------------------------------------
The original SAS does `%INC PGM(PBBLNFMT,PBBELF);` which defines two
PROC FORMAT catalogs used later:

  - CACBRCH.  : BRANCH -> CAC branch code (drives the CACBR='000' filter
                that Report A restricts to). WITHOUT THE REAL MAPPING,
                REPORT A'S POPULATION MAY BE WRONG.
  - ARRCLASS. : ARREAR2 -> display label (e.g. "2 - < 3 MTH"). WITHOUT
                THE REAL MAPPING, THE PRINTED ARREARS LABEL MAY BE WRONG,
                THOUGH IT DOES NOT AFFECT FILTERING OR TOTALS.

Both are implemented below as clearly-marked placeholder functions with a
best-effort guess inferred from the sample production output. Replace them
with the real catalog logic (e.g. from a lookup table you export from SAS)
before treating this as production-accurate.
"""

from pathlib import Path
from datetime import date, timedelta
import polars as pl
import pandas as pd
import pyreadstat
from typing import Dict, List, Optional

# ============================================================================
# 0. Paths / configuration
# ============================================================================

BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR301"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

LOANTEMP_PATH = INPUT_PATH / "BNM/LOANTEMP.sas7bdat"
LKP_BRANCH_PATH = INPUT_PATH / "LKP_BRANCH"   # flat file, no extension

# HPD product list - was macro variable &HPD in the original SAS. Set this
# to the real product code list used in production.
HPD_LIST = ["110", "115", "700", "705"]

BANK_TITLE = "P U B L I C   I S L A M I C   B A N K   B E R H A D"
LINE_WIDTH = 140


# ============================================================================
# 1. External-format placeholders (SEE WARNING ABOVE)
# ============================================================================

def cacbrch_format(branch: int) -> str:
    """
    TODO: replace with the real CACBRCH. mapping from PBBLNFMT/PBBELF.
    Placeholder guess: branches below 100 are non-CAC ('000'); everything
    else is tagged with its own branch number as a 3-digit CAC code.
    Report A's population (WHERE CACBR='000') depends entirely on this
    being correct.
    """
    if branch is None:
        return "UNK"
    return "000" if branch < 100 else f"{branch:03d}"


def arrclass_format(arrear2: Optional[float]) -> str:
    """
    TODO: replace with the real ARRCLASS. mapping from PBBLNFMT/PBBELF.
    Placeholder guess, inferred from the sample production output showing
    labels like "2 - < 3 MTH": bucket by whole month, one bucket per
    integer value of ARREAR2.
    """
    if arrear2 is None:
        return ""
    n = int(arrear2)
    return f"{n} - < {n + 1} MTH"


# ============================================================================
# 2. REPTDATE Processing with Previous Month Calculation (no control file)
# ============================================================================

def process_repdate() -> Dict[str, object]:
    """
    Build the reporting-date variables using today's date instead of the
    original BNM.REPTDATE control dataset. Previous month is derived with
    plain date/timedelta arithmetic: take the 1st of the current month and
    subtract one day to land on the last day of the previous month.
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
# 3. Load branch lookup (fixed-width, confirmed layout)
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """
    CONFIRMED layout from the original SAS INPUT statement:
        INPUT @2 BRANCH  3.
              @6 BRHCODE $3.;
    i.e. (1-indexed) columns 2-4 = numeric BRANCH, columns 6-8 = BRHCODE.
    In 0-indexed Python slicing that's [1:4] and [5:8].
    """
    if not LKP_BRANCH_PATH.exists():
        print(f"   WARNING: {LKP_BRANCH_PATH} not found - using empty branch lookup")
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

    rows = []
    with open(LKP_BRANCH_PATH, "r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            if len(raw_line) < 8:
                continue
            branch_str = raw_line[1:4].strip()
            brhcode = raw_line[5:8].strip()
            if not branch_str.isdigit():
                continue
            rows.append({"BRANCH": int(branch_str), "BRHCODE": brhcode})

    return pl.DataFrame(rows, schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})


# ============================================================================
# 4. LNTEMP: load loans, filter, left-join branch
# ============================================================================

def build_lntemp(hpd_list: List[str]) -> pl.DataFrame:
    """
    PROC SORT DATA=BNM.LOANTEMP OUT=LNTEMP;
      WHERE BALANCE > 0 AND BORSTAT NE 'Z' AND PRODUCT IN &HPD;
      BY BRANCH;
    ...merged with BRHDATA by BRANCH, keeping every LNTEMP row
    (IF PRESENT=1 THEN OUTPUT), which behaves like a LEFT JOIN off LNTEMP -
    CORRECTED: earlier draft used an inner join, which silently dropped any
    loan whose branch wasn't in the lookup file. A left join matches the
    SAS semantics (all qualifying loans are kept; BRHCODE is blank if the
    branch isn't found in the lookup).
    """
    loan_pdf, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_PATH))
    loan_df = pl.from_pandas(loan_pdf)
    loan_df = loan_df.rename({c: c.upper() for c in loan_df.columns})
    loan_df = loan_df.with_columns(pl.col("BRANCH").cast(pl.Int64, strict=False))

    hpd_numbers = [int(x.strip("'")) for x in hpd_list]

    filtered = loan_df.filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(hpd_numbers))
    ).sort("BRANCH")

    branch_df = load_branch_data()

    lntemp = filtered.join(branch_df, on="BRANCH", how="left").with_columns(
        pl.col("BRHCODE").fill_null("")
    )

    return lntemp


# ============================================================================
# 5. LOAN: arrears / new-loan population (literal SAS semantics: duplicates
#    are possible and intentional - a row satisfying both IF conditions is
#    output twice)
# ============================================================================

def create_loan(lntemp: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    DATA LOAN;
      SET LNTEMP;
      IF ARREAR2 GE 3 OR BORSTAT IN ('R','I','F','Y') THEN OUTPUT;
      IF ISSDTE GE &PREPTDTE AND DAYDIFF >= 8 THEN OUTPUT;
    RUN;

    CORRECTED: earlier draft applied .unique() across the two conditions.
    The real SAS DATA step has two independent IF/OUTPUT statements, so a
    row matching BOTH conditions is genuinely written twice. We preserve
    that here (no dedup) to match production row counts.
    """
    prept_date = variables['PREPTDTE']

    cond1 = lntemp.filter(
        (pl.col("ARREAR2") >= 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
    )
    cond2 = lntemp.filter(
        (pl.col("ISSDTE") >= prept_date) &
        (pl.col("DAYDIFF") >= 8)
    )

    return pl.concat([cond1, cond2], how="diagonal")


# ============================================================================
# 6. LOAN1: category assignment (literal SAS semantics: only 3 IF blocks,
#    each with its own OUTPUT - no default/"D" bucket, and a loan matching
#    more than one block's condition is intentionally duplicated)
# ============================================================================

def create_loan1(loan_df: pl.DataFrame) -> pl.DataFrame:
    """
    DATA LOAN1;
      SET LOAN;
      IF BORSTAT = 'F' THEN ARREAR2 = 15;
      ARREARS = PUT(ARREAR2, ARRCLASS.);
      CACBR   = PUT(BRANCH, CACBRCH.);
      IF PRODUCT IN (380,381,700,705,720,725) THEN DO;
         CAT='A'; TYPE='HP DIRECT(CONV) '; OUTPUT;
      END;
      IF PRODUCT IN (380,381) THEN DO;
         CAT='B'; TYPE='HP (380,381) '; OUTPUT;
      END;
      IF PRODUCT IN (128,130,131,132) THEN DO;
         CAT='C'; TYPE='AITAB '; OUTPUT;
      END;
    RUN;

    CORRECTED: earlier draft added a catch-all "D" category for anything
    not matching A/B/C, and treated the three conditions as mutually
    exclusive (if/elif). Neither is correct:
      - A loan with PRODUCT in (380,381) matches BOTH the CAT-A list and
        the CAT-B list, so it is intentionally emitted twice (once as A,
        once as B).
      - A loan with a PRODUCT not in any of the three lists is dropped
        from LOAN1 entirely (no OUTPUT happens for it at all).
    """
    base = loan_df.with_columns(
        pl.when(pl.col("BORSTAT") == "F")
        .then(pl.lit(15))
        .otherwise(pl.col("ARREAR2"))
        .alias("ARREAR2")
    )

    base = base.with_columns([
        pl.col("ARREAR2").map_elements(arrclass_format, return_dtype=pl.Utf8).alias("ARREARS"),
        pl.col("BRANCH").map_elements(cacbrch_format, return_dtype=pl.Utf8).alias("CACBR"),
    ])

    cat_a = base.filter(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725])) \
                .with_columns([pl.lit("A").alias("CAT"), pl.lit("HP DIRECT(CONV) ").alias("TYPE")])

    cat_b = base.filter(pl.col("PRODUCT").is_in([380, 381])) \
                .with_columns([pl.lit("B").alias("CAT"), pl.lit("HP (380,381) ").alias("TYPE")])

    cat_c = base.filter(pl.col("PRODUCT").is_in([128, 130, 131, 132])) \
                .with_columns([pl.lit("C").alias("CAT"), pl.lit("AITAB ").alias("TYPE")])

    loan1 = pl.concat([cat_a, cat_b, cat_c], how="diagonal")

    return loan1.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])


# ============================================================================
# 7. Fixed-column text line helpers (mimic SAS PUT @col ... FORMAT.)
# ============================================================================

def new_line(width: int = LINE_WIDTH) -> List[str]:
    return [" "] * width


def put_text(buf: List[str], col: int, text: str) -> None:
    """Left-justified write starting at 1-indexed column `col`."""
    text = "" if text is None else str(text)
    start = col - 1
    end = start + len(text)
    if end > len(buf):
        buf.extend([" "] * (end - len(buf)))
    buf[start:end] = list(text)


def put_num(buf: List[str], col: int, value, width: int, decimals: int = 0) -> None:
    """Right-justified numeric write within a field of `width` chars
    starting at 1-indexed column `col`, mimicking SAS COMMAw.d formats."""
    if value is None:
        formatted = ""
    else:
        formatted = f"{value:,.{decimals}f}"
    formatted = formatted[-width:] if len(formatted) > width else formatted
    put_text(buf, col + (width - len(formatted)), formatted)


def put_date(buf: List[str], col: int, d) -> None:
    """DDMMYY8. format: DD/MM/YY."""
    if d is None:
        return
    try:
        put_text(buf, col, d.strftime("%d/%m/%y"))
    except Exception:
        put_text(buf, col, "")


def render(buf: List[str]) -> str:
    return "".join(buf).rstrip()


# ============================================================================
# 8. Report A: AITAB/HP 2+ months arrears, non-CAC branches only
# ============================================================================

def generate_report_a(loan1_df: pl.DataFrame, variables: Dict) -> None:
    """
    WHERE CACBR = '000';
    BY CAT BRANCH ARREAR2 DESCENDING BALANCE;
    (loan1_df is already sorted this way from create_loan1)

    NOTE: the original SAS also forces a new page whenever LINECNT > 56
    mid-branch. That precise line-count-based repagination is NOT
    replicated here - this writer starts a new page per BRANCH (matching
    FIRST.BRANCH) but will not re-print the header mid-branch if a branch
    has enough accounts to overflow one printed page. Flag if you need
    that level of fidelity and it can be added.
    """
    data = loan1_df.filter(pl.col("CACBR") == "000")
    lines: List[str] = []

    if data.is_empty():
        print("   No data for non-CAC branches in Report A")
        (OUTPUT_PATH / "REPORT_A.txt").write_text("(no records)\n")
        return

    page = 0
    for cat in sorted(data["CAT"].unique().to_list()):
        cat_data = data.filter(pl.col("CAT") == cat)
        cat_total = 0.0
        cat_count = 0
        report_type = cat_data["TYPE"][0]

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            brhcode = branch_data["BRHCODE"][0]
            page += 1

            # --- page header ---
            b = new_line()
            put_text(b, 1, f"PROGRAM-ID:EIMAR301-A - BRANCH : {branch:3d}")
            put_text(b, 43, BANK_TITLE)
            put_text(b, 118, f"PAGE NO.: {page}")
            lines.append(render(b))

            b = new_line()
            put_text(b, 28, f"{report_type}2 MTHS & ABOVE AND A/C PAID 2 ISTL AND BELOW AS AT {variables['RDATE']}")
            lines.append(render(b))
            lines.append("")

            b = new_line()
            put_text(b, 1, "BRH"); put_text(b, 5, "NAME"); put_text(b, 25, "NOTENO")
            put_text(b, 34, "ISSUE DT"); put_text(b, 45, "LST TR DT")
            put_text(b, 61, "ISTL AMT"); put_text(b, 76, "NO ISTL PD")
            put_text(b, 87, "BORSTAT"); put_text(b, 95, "ARREARS")
            put_text(b, 115, "BALANCE")
            lines.append(render(b))

            b = new_line()
            put_text(b, 5, "ACC NO"); put_text(b, 25, "PRODUCT")
            put_text(b, 34, "MATURE DT"); put_text(b, 45, "LST TR AMT")
            put_text(b, 95, "DAYS ARR"); put_text(b, 115, "DELQ REASON CODE")
            lines.append(render(b))

            lines.append("    COLLATERAL DESC")
            lines.append("-" * 132)

            branch_total = 0.0
            branch_count = 0

            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arr_total = 0.0
                arr_count = 0

                for row in arrear_data.iter_rows(named=True):
                    b = new_line()
                    put_text(b, 1, row.get("BRHCODE", ""))
                    put_text(b, 5, row.get("NAME", ""))
                    put_text(b, 25, row.get("NOTENO", ""))
                    put_date(b, 34, row.get("ISSDTE"))
                    put_date(b, 52, row.get("LASTRAN"))
                    put_num(b, 61, row.get("PAYAMT"), 15, 2)
                    put_num(b, 77, row.get("NOISTLPD"), 8, 0)
                    put_text(b, 87, row.get("BORSTAT", ""))
                    put_text(b, 95, row.get("ARREARS", ""))
                    put_num(b, 114, row.get("BALANCE"), 17, 2)
                    lines.append(render(b))

                    b = new_line()
                    put_text(b, 5, row.get("ACCTNO", ""))
                    put_text(b, 25, str(row.get("PRODUCT", "")))
                    put_date(b, 34, row.get("MATURDT"))
                    put_num(b, 45, row.get("LSTTRNAM"), 15, 2)
                    put_num(b, 95, row.get("DAYDIFF"), 8, 0)
                    put_text(b, 114, row.get("DELQCD", ""))
                    lines.append(render(b))

                    lines.append(f"    {row.get('COLLDESC', '')}")

                    arr_total += row.get("BALANCE") or 0
                    arr_count += 1

                lines.append(("-" * 40).rjust(40 + 40) + ("-" * 10).rjust(10))
                b = new_line()
                put_text(b, 5, "SUBTOTAL")
                put_text(b, 41, f"NO OF A/C : {arr_count:>12,d}")
                put_num(b, 114, arr_total, 17, 2)
                lines.append(render(b))
                lines.append("-" * 132)
                lines.append("")

                branch_total += arr_total
                branch_count += arr_count

            lines.append("-" * 132)
            b = new_line()
            put_text(b, 5, "BRANCH TOTAL")
            put_text(b, 41, f"NO OF A/C : {branch_count:>12,d}")
            put_num(b, 114, branch_total, 17, 2)
            lines.append(render(b))
            lines.append("-" * 132)
            lines.append("")

            cat_total += branch_total
            cat_count += branch_count

        lines.append("-" * 132)
        b = new_line()
        put_text(b, 5, "GRAND TOTAL")
        put_text(b, 41, f"NO OF A/C : {cat_count:>12,d}")
        put_num(b, 114, cat_total, 17, 2)
        lines.append(render(b))
        lines.append("-" * 132)
        lines.append("")

    (OUTPUT_PATH / "REPORT_A.txt").write_text("\n".join(lines) + "\n")
    print(f"\u2713 Report A saved: {len(data)} accounts, {page} branch pages")


# ============================================================================
# 9. Report B: 3-8 months arrears, excludes BORSTAT F/I/R, ALL branches
#    (no CACBR restriction)
# ============================================================================

def generate_report_b(loan1_df: pl.DataFrame, variables: Dict) -> None:
    """
    WHERE (ARREAR2 >= 4 AND ARREAR2 < 10) AND
          (BORSTAT NE 'F' AND BORSTAT NE 'I' AND BORSTAT NE 'R');
    BY CAT BRANCH ARREAR2 DESCENDING BALANCE;

    CORRECTED: no CACBR='000' restriction here - Report B runs against the
    full LOAN1 population (both CAC and non-CAC branches).
    """
    data = loan1_df.filter(
        (pl.col("ARREAR2") >= 4) & (pl.col("ARREAR2") < 10) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )
    lines: List[str] = []

    if data.is_empty():
        print("   No data for Report B")
        (OUTPUT_PATH / "REPORT_B.txt").write_text("(no records)\n")
        return

    page = 0
    for cat in sorted(data["CAT"].unique().to_list()):
        cat_data = data.filter(pl.col("CAT") == cat)
        cat_total = 0.0
        cat_count = 0
        report_type = cat_data["TYPE"][0]

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            page += 1

            b = new_line()
            put_text(b, 1, f"PROGRAM-ID:EIMAR301-B - BRANCH : {branch:3d}")
            put_text(b, 43, BANK_TITLE)
            put_text(b, 110, f"PAGE NO.: {page}")
            lines.append(render(b))

            b = new_line()
            put_text(b, 28, f"{report_type}ACCOUNT WITH 3 - 8 MONTH IN ARREAR AS AT {variables['RDATE']}")
            lines.append(render(b))
            lines.append("")

            b = new_line()
            put_text(b, 1, "BRH"); put_text(b, 5, "ACCTNO"); put_text(b, 16, "NAME")
            put_text(b, 40, "NOTENO"); put_text(b, 50, "PRODUCT")
            put_text(b, 59, "BORSTAT"); put_text(b, 68, "ISSUE DT")
            put_text(b, 78, "DAYS"); put_text(b, 84, "ARREARS")
            put_text(b, 110, "BALANCE"); put_text(b, 120, "NO ISTL PAID")
            lines.append(render(b))

            b = new_line()
            put_text(b, 5, "LST TR DT"); put_text(b, 16, "MAT. DATE")
            put_text(b, 36, "LST TR AMT"); put_text(b, 49, "ISTL AMT")
            put_text(b, 59, "COLLATERAL DESCRIPTION")
            lines.append(render(b))
            lines.append("-" * 132)

            branch_total = 0.0
            branch_count = 0

            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arr_total = 0.0
                arr_count = 0

                for row in arrear_data.iter_rows(named=True):
                    b = new_line()
                    put_text(b, 1, row.get("BRHCODE", ""))
                    put_text(b, 5, row.get("ACCTNO", ""))
                    put_text(b, 16, row.get("NAME", ""))
                    put_text(b, 41, row.get("NOTENO", ""))
                    put_text(b, 54, str(row.get("PRODUCT", "")))
                    put_text(b, 59, row.get("BORSTAT", ""))
                    put_date(b, 68, row.get("ISSDTE"))
                    put_num(b, 79, row.get("DAYDIFF"), 5, 0)
                    put_text(b, 84, row.get("ARREARS", ""))
                    put_num(b, 100, row.get("BALANCE"), 17, 2)
                    put_num(b, 120, row.get("NOISTLPD"), 10, 0)
                    lines.append(render(b))

                    b = new_line()
                    put_date(b, 5, row.get("LASTRAN"))
                    put_date(b, 16, row.get("MATURDT"))
                    put_num(b, 29, row.get("LSTTRNAM"), 17, 2)
                    put_num(b, 46, row.get("PAYAMT"), 11, 2)
                    put_text(b, 59, row.get("COLLDESC", ""))
                    lines.append(render(b))

                    arr_total += row.get("BALANCE") or 0
                    arr_count += 1

                lines.append("-" * 132)
                b = new_line()
                put_text(b, 5, "SUBTOTAL")
                put_text(b, 41, f"NO OF A/C : {arr_count:>12,d}")
                put_num(b, 100, arr_total, 17, 2)
                lines.append(render(b))
                lines.append("-" * 132)
                lines.append("")

                branch_total += arr_total
                branch_count += arr_count

            lines.append("-" * 132)
            b = new_line()
            put_text(b, 5, "BRANCH TOTAL")
            put_text(b, 41, f"NO OF A/C : {branch_count:>12,d}")
            put_num(b, 100, branch_total, 17, 2)
            lines.append(render(b))
            lines.append("-" * 132)
            lines.append("")

            cat_total += branch_total
            cat_count += branch_count

        lines.append("-" * 132)
        b = new_line()
        put_text(b, 5, "GRAND TOTAL")
        put_text(b, 41, f"NO OF A/C : {cat_count:>12,d}")
        put_num(b, 100, cat_total, 17, 2)
        lines.append(render(b))
        lines.append("-" * 132)
        lines.append("")

    (OUTPUT_PATH / "REPORT_B.txt").write_text("\n".join(lines) + "\n")
    print(f"\u2713 Report B saved: {len(data)} accounts, {page} branch pages")


# ============================================================================
# 10. Report C / D: PROC TABULATE-style crosstab from LNTEMP
# ============================================================================

def _payment_pivot(df: pl.DataFrame, variables: Dict, title2: str, title3_suffix: str,
                    include_total_col: bool, out_name: str) -> None:
    """Shared crosstab writer for Reports C and D: rows = BRHCODE (+ TOTAL),
    columns = PAYDESC categories, cells = NO OF A/C and O/S BALANCE."""
    if df.is_empty():
        print(f"   No data for {out_name}")
        (OUTPUT_PATH / f"{out_name}.txt").write_text("(no records)\n")
        return

    paydesc_order = [p for p in ["NO PAYMENT", "PAID 1 ISTL", "PAID 2 ISTL"]
                      if p in df["PAYDESC"].unique().to_list()]

    summary = df.group_by(["BRHCODE", "PAYDESC"]).agg([
        pl.count().alias("NOACCT"),
        pl.sum("BALANCE").alias("BAL"),
    ])

    branches = sorted(df["BRHCODE"].unique().to_list())

    lines = []
    lines.append(f"PROGRAM ID : {out_name.replace('REPORT_', 'EIMAR301-')}")
    lines.append("PUBLIC ISLAMIC BANK BERHAD")
    lines.append(f"{title2} AS AT {variables['RDATE']}")
    lines.append("")

    header = f"{'BRANCH':<8}"
    for p in paydesc_order:
        header += f"{p + ' NO OF A/C':>16}{p + ' O/S BAL':>18}"
    if include_total_col:
        header += f"{'TOTAL NO OF A/C':>18}{'TOTAL O/S BAL':>18}"
    lines.append(header)
    lines.append("-" * len(header))

    grand_counts = {p: 0 for p in paydesc_order}
    grand_bal = {p: 0.0 for p in paydesc_order}

    for brh in branches:
        row_str = f"{brh:<8}"
        row_total_count = 0
        row_total_bal = 0.0
        for p in paydesc_order:
            cell = summary.filter((pl.col("BRHCODE") == brh) & (pl.col("PAYDESC") == p))
            cnt = cell["NOACCT"][0] if len(cell) > 0 else 0
            bal = cell["BAL"][0] if len(cell) > 0 else 0.0
            row_str += f"{cnt:>16,d}{bal:>18,.2f}"
            row_total_count += cnt
            row_total_bal += bal
            grand_counts[p] += cnt
            grand_bal[p] += bal
        if include_total_col:
            row_str += f"{row_total_count:>18,d}{row_total_bal:>18,.2f}"
        lines.append(row_str)

    total_row = f"{'TOTAL':<8}"
    grand_total_count = 0
    grand_total_bal = 0.0
    for p in paydesc_order:
        total_row += f"{grand_counts[p]:>16,d}{grand_bal[p]:>18,.2f}"
        grand_total_count += grand_counts[p]
        grand_total_bal += grand_bal[p]
    if include_total_col:
        total_row += f"{grand_total_count:>18,d}{grand_total_bal:>18,.2f}"
    lines.append("-" * len(header))
    lines.append(total_row)

    (OUTPUT_PATH / f"{out_name}.txt").write_text("\n".join(lines) + "\n")
    print(f"\u2713 {out_name} saved: {len(df)} accounts across {len(branches)} branches")


def create_report_c_data(lntemp: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """
    DATA NEWREL; SET LNTEMP;
      IF ISSDTE GE &PREPTDTE AND DAYDIFF >= 8;
      IF NOISTLPD LT 1 THEN PAYDESC='NO PAYMENT';
      ELSE IF (NOISTLPD GE 1 AND NOISTLPD LT 2) THEN PAYDESC='PAID 1 ISTL';
      ELSE PAYDESC='PAID 2 ISTL';

    CORRECTED: "PAID 2 ISTL" here has NO upper bound - any NOISTLPD >= 2
    falls in this bucket (earlier draft implicitly assumed an upper cap).
    """
    new_rel = lntemp.filter(
        (pl.col("ISSDTE") >= variables['PREPTDTE']) & (pl.col("DAYDIFF") >= 8)
    )
    return new_rel.with_columns(
        pl.when(pl.col("NOISTLPD") < 1).then(pl.lit("NO PAYMENT"))
        .when((pl.col("NOISTLPD") >= 1) & (pl.col("NOISTLPD") < 2)).then(pl.lit("PAID 1 ISTL"))
        .otherwise(pl.lit("PAID 2 ISTL"))
        .alias("PAYDESC")
    )


def create_report_d_data(lntemp: pl.DataFrame) -> pl.DataFrame:
    """
    DATA ACCARR; SET LNTEMP;
      IF (2<=NOISTLPD<3) AND DAYDIFF >= 8;
      PAYDESC = 'PAID 2 ISTL';

    CORRECTED: filter is NOISTLPD in [2, 3) exactly - earlier draft used
    NOISTLPD >= 2 with no upper bound, which overlapped with Report C's
    open-ended "PAID 2 ISTL" bucket instead of matching this dataset's
    narrower definition.
    """
    acc_arr = lntemp.filter(
        (pl.col("NOISTLPD") >= 2) & (pl.col("NOISTLPD") < 3) & (pl.col("DAYDIFF") >= 8)
    )
    return acc_arr.with_columns(pl.lit("PAID 2 ISTL").alias("PAYDESC"))


# ============================================================================
# 11. Main Execution
# ============================================================================

def main():
    print("=" * 60)
    print("EIMAR301 / EIMIR301 SAS to Python Conversion")
    print("=" * 60)

    print("\n1. Processing REPTDATE with previous month...")
    variables = process_repdate()
    print(f"   Current Date: {variables['RDATE']}")
    print(f"   Previous Month Date: {variables['PREPTDTE']}")

    print("\n2. Building LNTEMP (filtered loans + branch lookup)...")
    lntemp = build_lntemp(HPD_LIST)
    print(f"   LNTEMP rows: {len(lntemp)}")

    print("\n3. Building LOAN (arrears / new-loan population, duplicates preserved)...")
    loan = create_loan(lntemp, variables)
    print(f"   LOAN rows: {len(loan)}")

    print("\n4. Building LOAN1 (category assignment, duplicates preserved, no default cat)...")
    loan1 = create_loan1(loan)
    print(f"   LOAN1 rows: {len(loan1)}")

    print("\n5. Generating Report A (EIMAR301-A, non-CAC branches only)...")
    generate_report_a(loan1, variables)

    print("\n6. Generating Report B (EIMAR301-B, all branches)...")
    generate_report_b(loan1, variables)

    print("\n7. Generating Report C (EIMAR301-C, new releases payment summary)...")
    report_c_data = create_report_c_data(lntemp, variables)
    _payment_pivot(
        report_c_data, variables,
        title2="SUMMARY ON AC WITH PAYMENT OF 2 ISTL & BELOW",
        title3_suffix="",
        include_total_col=True,
        out_name="REPORT_C",
    )

    print("\n8. Generating Report D (EIMAR301-D, exactly 2 installments paid)...")
    report_d_data = create_report_d_data(lntemp)
    _payment_pivot(
        report_d_data, variables,
        title2="SUMMARY ON A/C IN ARREAR WITH 2 ISTL PAID ONLY",
        title3_suffix="",
        include_total_col=False,
        out_name="REPORT_D",
    )

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Output saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
