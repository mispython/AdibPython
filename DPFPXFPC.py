"""
EIMAR301 SAS to Python conversion
Multi-report system for HP Direct loan analysis with different criteria

SAS FORMAT DEPENDENCIES
-----------------------
The SAS source uses:
    %INC PGM(PBBLNFMT,PBBELF);
    PRODUCT IN &HPD;
    ARREARS = PUT(ARREAR2, ARRCLASS.);
    CACBR   = PUT(BRANCH,  CACBRCH.);
    ... WHERE CACBR = '000';

This Python port calls the equivalent Python modules directly:

    PBBLNFMT.HPD                          -> &HPD macro
    PBBLNFMT.put(value, 'ARRCLASS')       -> ARREARS label
    PBBELF.format_cacbrch(value)          -> CACBR (911/912/913/
                                             914/915/916/000)

Both modules are MANDATORY. The script aborts at startup if either
fails to import or does not expose the required entry point.

LOGIC (matches SAS original)
----------------------------
A. Report A/B CAT assignment: emit one row PER matching
   "IF PRODUCT IN (...) THEN OUTPUT" block (380/381 emit BOTH A and B).
B. BORSTAT='F' => ARREAR2_ADJ = 15, applied BEFORE PUT ARRCLASS,
   BEFORE sorting, BEFORE subtotaling.
C. Sorts use ARREAR2_ADJ (Report A) / raw ARREAR2 (Report B).
D. Report A listing filters WHERE CACBR='000' using PBBELF.format_cacbrch.
E. ARREARS label comes from PBBLNFMT.put(ARREAR2_ADJ, 'ARRCLASS').
F. The two "IF ... THEN OUTPUT" in DATA LOAN are concatenated without
   unique() to preserve SAS-legal duplicate rows.
G. Report C and D use PROC TABULATE-style wide layout.
H. All summary-row builders explicitly cast every field to a single
   Python type (str/int/float) so pl.DataFrame() does not hit
   ComputeError when a column mixes numeric subtotal keys with
   'ALL'/'TOTAL' string labels. infer_schema_length=None as belt-
   and-braces.
"""

from pathlib import Path
from datetime import date, timedelta
import re
import importlib
from typing import Dict, List, Callable

import polars as pl
import pandas as pd
import pyreadstat

# ============================================================================
# 0. Paths / configuration
# ============================================================================

LOANTEMP_PATH   = Path("/dwh/dwh_m/tempsource/loantemp.sas7bdat")
LKP_BRANCH_PATH = Path("/sasdata/rawdata/lookup/LKP_BRANCH")
OUTPUT_PATH     = Path("/host_pq/mis/output/report/EIMIR")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

USE_FIXED_WIDTH = False
FIXED_WIDTH_SPECS = {
    "colspecs": [(0, 6), (7, 15)],
    "names": ["BRANCH", "BRHCODE"],
}

TEXT_COL_WIDTH = 15
TEXT_DELIM     = "|"

# Product -> (CAT, TYPE) rules, mirrors SAS IF PRODUCT IN (...) THEN OUTPUT
PRODUCT_CATEGORY_RULES = [
    ("A", "HP DIRECT(CONV) ", [380, 381, 700, 705, 720, 725]),
    ("B", "HP (380,381)    ", [380, 381]),
    ("C", "AITAB           ", [128, 130, 131, 132]),
]


# ============================================================================
# 1. Load the two SAS-format subprograms (mandatory)
# ============================================================================

def _must_import(name: str):
    """Import a module by name; raise if it is missing."""
    try:
        return importlib.import_module(name)
    except Exception as e:
        raise ImportError(
            f"FATAL: required format module '{name}' could not be imported: {e}"
        ) from e


PBBLNFMT = _must_import("PBBLNFMT")
PBBELF   = _must_import("PBBELF")


# ============================================================================
# 2. Resolve the SAS macros / formats strictly from the modules
# ============================================================================

def _resolve_hpd() -> List[int]:
    """SAS: PRODUCT IN &HPD;  (&HPD lives in PBBLNFMT)."""
    hpd = getattr(PBBLNFMT, "HPD", None)
    if not isinstance(hpd, (list, tuple)) or not hpd:
        raise AttributeError(
            "FATAL: PBBLNFMT does not expose a non-empty HPD macro list."
        )
    return [int(x) for x in hpd]


def _resolve_arrclass() -> Callable[[object], str]:
    """SAS: ARREARS = PUT(ARREAR2_ADJ, ARRCLASS.) via PBBLNFMT.put."""
    put_fn    = getattr(PBBLNFMT, "put", None)
    formats_d = getattr(PBBLNFMT, "FORMATS", None)
    if not callable(put_fn):
        raise AttributeError(
            "FATAL: PBBLNFMT does not expose put(); cannot apply ARRCLASS."
        )
    if not isinstance(formats_d, dict) or "ARRCLASS" not in formats_d:
        raise AttributeError(
            "FATAL: PBBLNFMT.FORMATS does not contain 'ARRCLASS'."
        )

    def _apply(v):
        if v is None:
            return ""
        try:
            return str(put_fn(v, "ARRCLASS"))
        except Exception as e:
            raise RuntimeError(
                f"FATAL: PBBLNFMT.put({v!r}, 'ARRCLASS') failed: {e}"
            ) from e
    return _apply


def _resolve_cacbrch() -> Callable[[object], str]:
    """SAS: CACBR = PUT(BRANCH, CACBRCH.) via PBBELF.format_cacbrch."""
    fn = getattr(PBBELF, "format_cacbrch", None)
    if not callable(fn):
        raise AttributeError(
            "FATAL: PBBELF does not expose format_cacbrch(); "
            "cannot apply CACBRCH."
        )

    def _apply(v):
        if v is None:
            return "000"
        try:
            return str(fn(int(v)))
        except Exception as e:
            raise RuntimeError(
                f"FATAL: PBBELF.format_cacbrch({v!r}) failed: {e}"
            ) from e
    return _apply


HPD_LIST:     List[int]               = _resolve_hpd()
ARRCLASS_FMT: Callable[[object], str] = _resolve_arrclass()
CACBRCH_FMT:  Callable[[object], str] = _resolve_cacbrch()


def _describe_resolution() -> None:
    """Print how each macro/format was bound, plus a few spot-check values."""
    print(f"   PBBLNFMT : {getattr(PBBLNFMT, '__name__', '?')} "
          f"[{getattr(PBBLNFMT, '__file__', '?')}]")
    print(f"   PBBELF   : {getattr(PBBELF,   '__name__', '?')} "
          f"[{getattr(PBBELF,   '__file__', '?')}]")
    print(f"   HPD      bound via: PBBLNFMT.HPD = {HPD_LIST}")
    print("   ARRCLASS bound via: PBBLNFMT.put(v,'ARRCLASS')")
    print(f"     ARRCLASS( 2) = {ARRCLASS_FMT(2)!r}   (expected '1 - < 2 MTH   ')")
    print(f"     ARRCLASS( 3) = {ARRCLASS_FMT(3)!r}   (expected '2 - < 3 MTH   ')")
    print(f"     ARRCLASS(15) = {ARRCLASS_FMT(15)!r}  (expected 'DEFICIT       ')")
    print("   CACBRCH  bound via: PBBELF.format_cacbrch(v)")
    print(f"     CACBRCH(  2) = {CACBRCH_FMT(2)!r}   (expected '911' - KL)")
    print(f"     CACBRCH( 13) = {CACBRCH_FMT(13)!r}  (expected '000' - non-CAC)")
    print(f"     CACBRCH( 31) = {CACBRCH_FMT(31)!r}  (expected '000' - non-CAC)")


# ============================================================================
# 3. Reporting-date variables
# ============================================================================

def process_repdate() -> Dict[str, object]:
    repdate             = date.today()
    first_of_this_month = repdate.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    pmth  = last_day_prev_month.month
    pyear = last_day_prev_month.year
    pdate = date(pyear, pmth, 1)

    return {
        'RDATE':    repdate.strftime("%d/%m/%y"),
        'REPTYEAR': str(repdate.year),
        'REPTMON':  f"{repdate.month:02d}",
        'REPTDAY':  f"{repdate.day:02d}",
        'REPTDATE': repdate,
        'PREPTDTE': pdate,
        'PMTH':     pmth,
        'PYEAR':    pyear,
    }


# ============================================================================
# 4. Data loaders
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    if not LKP_BRANCH_PATH.exists():
        print(f"   WARNING: {LKP_BRANCH_PATH} not found - using empty lookup")
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
        rows, raw_lines_sample = [], []
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
                digits = re.sub(r"\D", "", tokens[0])
                if digits == "":
                    continue
                rows.append({"BRANCH": int(digits), "BRHCODE": tokens[1]})
        print("   [debug] LKP_BRANCH first 8 raw lines (repr):")
        for rl in raw_lines_sample:
            print(f"   [debug]   {rl!r}")
        print(f"   [debug] first 8 parsed rows: {rows[:8]}")
        df = pl.DataFrame(rows, schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

    rename_map = {}
    for col in df.columns:
        u = col.strip().upper()
        if u in ("BRANCH", "BRANCH_NO", "BR_NO"):
            rename_map[col] = "BRANCH"
        elif u in ("BRHCODE", "BR_CODE", "BRANCH_CODE"):
            rename_map[col] = "BRHCODE"
    if rename_map:
        df = df.rename(rename_map)

    return df.with_columns(pl.col("BRANCH").cast(pl.Int64, strict=False))


def load_and_filter_loans(hpd_list: List[int], variables: Dict) -> pl.DataFrame:
    loan_pdf, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_PATH))
    loan_df = pl.from_pandas(loan_pdf)
    loan_df = loan_df.rename({c: c.upper() for c in loan_df.columns})

    # Force numeric identity columns to Int64 (SAS numerics come back as f64)
    loan_df = loan_df.with_columns([
        pl.col("BRANCH").cast(pl.Int64, strict=False),
        pl.col("PRODUCT").cast(pl.Int64, strict=False),
    ])

    print(f"   [debug] raw LOANTEMP rows: {len(loan_df)}")
    print(f"   [debug] BALANCE>0 rows: {len(loan_df.filter(pl.col('BALANCE') > 0))}")
    print(f"   [debug] BORSTAT != 'Z' rows: {len(loan_df.filter(pl.col('BORSTAT') != 'Z'))}")
    print(f"   [debug] sample PRODUCT values: {loan_df['PRODUCT'].unique().to_list()[:15]}")
    print(f"   [debug] sample BORSTAT values: {loan_df['BORSTAT'].unique().to_list()[:15]}")
    print(f"   [debug] HPD filter numbers (from PBBLNFMT.HPD): {hpd_list}")
    print(f"   [debug] PRODUCT in HPD rows: {len(loan_df.filter(pl.col('PRODUCT').is_in(hpd_list)))}")

    filtered = loan_df.filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(hpd_list))
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
# 5. Text writer
# ============================================================================

def write_text_report(df: pl.DataFrame, filepath: Path, title: str = "") -> None:
    lines = []
    if title:
        lines.append(title)
        lines.append("=" * max(len(title), 40))

    if df is None or df.is_empty():
        lines.append("(no records)")
        filepath.write_text("\n".join(lines) + "\n")
        return

    columns = df.columns
    header  = TEXT_DELIM.join(f"{c:<{TEXT_COL_WIDTH}}" for c in columns)
    lines.append(header)
    lines.append("-" * len(header))
    for row in df.iter_rows():
        lines.append(TEXT_DELIM.join(f"{str(v):<{TEXT_COL_WIDTH}}" for v in row))
    filepath.write_text("\n".join(lines) + "\n")


# ============================================================================
# 6. Category explosion (SAS: multiple IF ... OUTPUT blocks)
# ============================================================================

def explode_categories(df: pl.DataFrame) -> pl.DataFrame:
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
        return df.head(0).with_columns([
            pl.lit(None).cast(pl.Utf8).alias("CAT"),
            pl.lit(None).cast(pl.Utf8).alias("TYPE"),
        ])
    return pl.concat(parts, how="diagonal")


# ============================================================================
# 7. Report A
# ============================================================================

def create_report_a_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    # DATA LOAN: two IF ... THEN OUTPUT (no unique — duplicates legal)
    cond1 = loan_df.filter(
        (pl.col("ARREAR2") >= 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
    )
    cond2 = loan_df.filter(
        (pl.col("ISSDTE") >= variables['PREPTDTE']) &
        (pl.col("DAYDIFF") >= 8)
    )
    loan = pl.concat([cond1, cond2], how="diagonal")

    # IF BORSTAT='F' THEN ARREAR2=15;  (BEFORE PUT ARRCLASS & sort)
    loan = loan.with_columns(
        pl.when(pl.col("BORSTAT") == "F")
        .then(pl.lit(15))
        .otherwise(pl.col("ARREAR2"))
        .alias("ARREAR2_ADJ")
    )

    # ARREARS = PUT(ARREAR2_ADJ, ARRCLASS.)  -> PBBLNFMT.put
    # CACBR   = PUT(BRANCH, CACBRCH.)        -> PBBELF.format_cacbrch
    loan = loan.with_columns([
        pl.col("ARREAR2_ADJ").map_elements(ARRCLASS_FMT, return_dtype=pl.Utf8).alias("ARREARS"),
        pl.col("BRANCH").map_elements(CACBRCH_FMT,      return_dtype=pl.Utf8).alias("CACBR"),
    ])

    categorized = explode_categories(loan)
    if categorized.is_empty():
        return categorized

    # PROC SORT BY CAT BRANCH ARREAR2_ADJ DESCENDING BALANCE
    return categorized.sort(
        ["CAT", "BRANCH", "ARREAR2_ADJ", "BALANCE"],
        descending=[False, False, False, True],
    )


def generate_report_a_summary(report_a_df: pl.DataFrame, variables: Dict):
    # WHERE CACBR='000'  (real mapping via PBBELF.format_cacbrch)
    non_cac = report_a_df.filter(pl.col("CACBR") == "000")

    if non_cac.is_empty():
        print("   No data for non-CAC branches in Report A")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
        return

    summary_data = []
    grand_bal, grand_ac = 0.0, 0

    for cat in sorted(non_cac["CAT"].unique().to_list()):
        cat_data = non_cac.filter(pl.col("CAT") == cat)
        cat_bal, cat_ac = 0.0, 0

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            bd = cat_data.filter(pl.col("BRANCH") == branch)
            brhcode = bd["BRHCODE"][0] if len(bd) > 0 else ""

            branch_bal, branch_ac = 0.0, 0

            for arr in sorted(bd["ARREAR2_ADJ"].unique().to_list()):
                ad = bd.filter(pl.col("ARREAR2_ADJ") == arr)
                ad_bal = ad["BALANCE"].sum()
                ad_ac  = len(ad)

                summary_data.append({
                    "REPORT":        "A_SUBTOTAL",
                    "CATEGORY":      str(cat),
                    "BRANCH":        str(branch),
                    "BRHCODE":       str(brhcode),
                    "ARREAR_BUCKET": str(arr),
                    "ACCOUNT_COUNT": int(ad_ac),
                    "TOTAL_BALANCE": float(ad_bal),
                    "ARREARS_CLASS": str(ad["ARREARS"][0]) if len(ad) > 0 else "",
                })
                branch_bal += ad_bal
                branch_ac  += ad_ac

            summary_data.append({
                "REPORT":        "A_BRANCH_TOTAL",
                "CATEGORY":      str(cat),
                "BRANCH":        str(branch),
                "BRHCODE":       str(brhcode),
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": int(branch_ac),
                "TOTAL_BALANCE": float(branch_bal),
                "ARREARS_CLASS": "BRANCH TOTAL",
            })
            cat_bal += branch_bal
            cat_ac  += branch_ac

        summary_data.append({
            "REPORT":        "A_GRAND_TOTAL",
            "CATEGORY":      str(cat),
            "BRANCH":        "ALL",
            "BRHCODE":       "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": int(cat_ac),
            "TOTAL_BALANCE": float(cat_bal),
            "ARREARS_CLASS": "GRAND TOTAL",
        })
        grand_bal += cat_bal
        grand_ac  += cat_ac

    summary_data.append({
        "REPORT":        "A_OVERALL_GRAND",
        "CATEGORY":      "ALL",
        "BRANCH":        "ALL",
        "BRHCODE":       "",
        "ARREAR_BUCKET": "ALL",
        "ACCOUNT_COUNT": int(grand_ac),
        "TOTAL_BALANCE": float(grand_bal),
        "ARREARS_CLASS": "OVERALL TOTAL",
    })

    summary_df = pl.DataFrame(summary_data, infer_schema_length=None)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_A_SUMMARY.txt", "REPORT A SUMMARY")
    print(f"\u2713 Report A summary saved: {len(summary_df)} records")


# ============================================================================
# 8. Report B
# ============================================================================

def create_report_b_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    b = loan_df.filter(
        (pl.col("ARREAR2") >= 4) &
        (pl.col("ARREAR2") < 10) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )
    categorized = explode_categories(b)
    if categorized.is_empty():
        return categorized
    return categorized.sort(
        ["CAT", "BRANCH", "ARREAR2", "BALANCE"],
        descending=[False, False, False, True],
    )


def generate_report_b_summary(report_b_df: pl.DataFrame, variables: Dict):
    if report_b_df.is_empty():
        print("   No data for Report B")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
        return

    summary_data = []
    grand_bal, grand_ac = 0.0, 0

    for cat in sorted(report_b_df["CAT"].unique().to_list()):
        cat_data = report_b_df.filter(pl.col("CAT") == cat)
        cat_bal, cat_ac = 0.0, 0

        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            bd = cat_data.filter(pl.col("BRANCH") == branch)
            brhcode = bd["BRHCODE"][0] if len(bd) > 0 else ""

            branch_bal, branch_ac = 0.0, 0

            for arr in sorted(bd["ARREAR2"].unique().to_list()):
                ad = bd.filter(pl.col("ARREAR2") == arr)
                ad_bal = ad["BALANCE"].sum()
                ad_ac  = len(ad)

                summary_data.append({
                    "REPORT":        "B_SUBTOTAL",
                    "CATEGORY":      str(cat),
                    "BRANCH":        str(branch),
                    "BRHCODE":       str(brhcode),
                    "ARREAR_BUCKET": str(arr),
                    "ACCOUNT_COUNT": int(ad_ac),
                    "TOTAL_BALANCE": float(ad_bal),
                })
                branch_bal += ad_bal
                branch_ac  += ad_ac

            summary_data.append({
                "REPORT":        "B_BRANCH_TOTAL",
                "CATEGORY":      str(cat),
                "BRANCH":        str(branch),
                "BRHCODE":       str(brhcode),
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": int(branch_ac),
                "TOTAL_BALANCE": float(branch_bal),
            })
            cat_bal += branch_bal
            cat_ac  += branch_ac

        summary_data.append({
            "REPORT":        "B_GRAND_TOTAL",
            "CATEGORY":      str(cat),
            "BRANCH":        "ALL",
            "BRHCODE":       "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": int(cat_ac),
            "TOTAL_BALANCE": float(cat_bal),
        })
        grand_bal += cat_bal
        grand_ac  += cat_ac

    summary_data.append({
        "REPORT":        "B_OVERALL_GRAND",
        "CATEGORY":      "ALL",
        "BRANCH":        "ALL",
        "BRHCODE":       "",
        "ARREAR_BUCKET": "ALL",
        "ACCOUNT_COUNT": int(grand_ac),
        "TOTAL_BALANCE": float(grand_bal),
    })

    summary_df = pl.DataFrame(summary_data, infer_schema_length=None)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_B_SUMMARY.txt", "REPORT B SUMMARY")
    print(f"\u2713 Report B summary saved: {len(summary_df)} records")


# ============================================================================
# 9. Report C
# ============================================================================

def create_report_c_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
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
        row = {"BRHCODE": str(brhcode)}
        total_ac, total_bal = 0, 0.0
        for pd_ in paydescs:
            sub = bh.filter(pl.col("PAYDESC") == pd_)
            ac  = int(sub["NOACCT"].sum()) if len(sub) > 0 else 0
            bal = float(sub["BALANCE_SUM"].sum()) if len(sub) > 0 else 0.0
            row[f"{pd_} :: NO_OF_AC"]    = ac
            row[f"{pd_} :: O_S_BALANCE"] = bal
            total_ac  += ac
            total_bal += bal
        row["TOTAL :: NO_OF_AC"]    = int(total_ac)
        row["TOTAL :: O_S_BALANCE"] = float(total_bal)
        rows.append(row)

    rows.append({
        "BRHCODE": "TOTAL",
        **{k: sum(r[k] for r in rows) for k in rows[0] if k != "BRHCODE"},
    })

    summary_df = pl.DataFrame(rows, infer_schema_length=None)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_C_SUMMARY.txt", "REPORT C SUMMARY")
    print(f"\u2713 Report C summary saved: {len(summary_df)} records")


# ============================================================================
# 10. Report D
# ============================================================================

def create_report_d_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    d = loan_df.filter(
        (pl.col("NOISTLPD") >= 2) &
        (pl.col("NOISTLPD") < 3) &
        (pl.col("DAYDIFF") >= 8)
    )
    return d.with_columns(pl.lit("PAID 2 ISTL").alias("PAYDESC"))


def generate_report_d_summary(report_d_df: pl.DataFrame, variables: Dict):
    if report_d_df.is_empty():
        print("   No data for Report D")
        write_text_report(pl.DataFrame(), OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
        return

    summary = report_d_df.group_by("BRHCODE").agg([
        pl.len().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM"),
    ]).sort("BRHCODE")

    rows = []
    total_ac, total_bal = 0, 0.0
    for row in summary.iter_rows(named=True):
        rows.append({
            "BRHCODE":     str(row["BRHCODE"]),
            "PAYDESC":     "PAID 2 ISTL",
            "NO_OF_AC":    int(row["NOACCT"]),
            "O_S_BALANCE": float(row["BALANCE_SUM"]),
        })
        total_ac  += int(row["NOACCT"])
        total_bal += float(row["BALANCE_SUM"])

    rows.append({
        "BRHCODE":     "TOTAL",
        "PAYDESC":     "PAID 2 ISTL",
        "NO_OF_AC":    int(total_ac),
        "O_S_BALANCE": float(total_bal),
    })

    summary_df = pl.DataFrame(rows, infer_schema_length=None)
    write_text_report(summary_df, OUTPUT_PATH / "REPORT_D_SUMMARY.txt", "REPORT D SUMMARY")
    print(f"\u2713 Report D summary saved: {len(summary_df)} records")


# ============================================================================
# 11. Main
# ============================================================================

def main():
    print("=" * 60)
    print("EIMAR301 SAS to Python Conversion - Multi-Report System")
    print("=" * 60)

    _describe_resolution()

    print("\n1. Processing REPTDATE with previous month (datetime/timedelta)...")
    variables = process_repdate()
    print(f"   Current Date: {variables['RDATE']}")
    print(f"   Previous Month Date: {variables['PREPTDTE']}")

    print("\n2. Loading and filtering HP Direct loans...")
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
    print(f"   Filtered HP Direct loans: {len(filtered_loans)}")

    print("\n3. Generating Report A (EIMAR301-A)...")
    report_a_data = create_report_a_data(filtered_loans, variables)
    print(f"   Report A accounts: {len(report_a_data)}")
    generate_report_a_summary(report_a_data, variables)

    print("\n4. Generating Report B (EIMAR301-B)...")
    report_b_data = create_report_b_data(filtered_loans, variables)
    print(f"   Report B accounts: {len(report_b_data)}")
    generate_report_b_summary(report_b_data, variables)

    print("\n5. Generating Report C (EIMAR301-C)...")
    report_c_data = create_report_c_data(filtered_loans, variables)
    print(f"   Report C accounts: {len(report_c_data)}")
    generate_report_c_summary(report_c_data, variables)

    print("\n6. Generating Report D (EIMAR301-D)...")
    report_d_data = create_report_d_data(filtered_loans, variables)
    print(f"   Report D accounts: {len(report_d_data)}")
    generate_report_d_summary(report_d_data, variables)

    print("\n7. Creating combined analysis...")
    overall_stats = {
        "TOTAL_HP_LOANS":    len(filtered_loans),
        "REPORT_A_ACCOUNTS": len(report_a_data),
        "REPORT_B_ACCOUNTS": len(report_b_data),
        "REPORT_C_ACCOUNTS": len(report_c_data),
        "REPORT_D_ACCOUNTS": len(report_d_data),
        "TOTAL_BALANCE":     float(filtered_loans["BALANCE"].sum()),
        "AVG_BALANCE":       float(filtered_loans["BALANCE"].mean()),
        "AVG_ARREAR":        float(filtered_loans["ARREAR2"].mean()),
        "NEW_LOANS_COUNT":   len(filtered_loans.filter(pl.col("ISSDTE") >= variables['PREPTDTE'])),
        "NPL_COUNT":         len(filtered_loans.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        )),
    }
    write_text_report(pl.DataFrame([overall_stats]),
                      OUTPUT_PATH / "OVERALL_STATISTICS.txt",
                      "OVERALL STATISTICS")

    print("\n8. Creating detailed data extracts...")
    write_text_report(report_a_data, OUTPUT_PATH / "REPORT_A_DETAILED.txt", "REPORT A DETAILED")
    write_text_report(report_b_data, OUTPUT_PATH / "REPORT_B_DETAILED.txt", "REPORT B DETAILED")
    write_text_report(report_c_data, OUTPUT_PATH / "REPORT_C_DETAILED.txt", "REPORT C DETAILED")
    write_text_report(report_d_data, OUTPUT_PATH / "REPORT_D_DETAILED.txt", "REPORT D DETAILED")

    print("\n9. Creating branch performance analysis...")
    branch_metrics = []
    for branch in filtered_loans["BRANCH"].unique().to_list():
        bd = filtered_loans.filter(pl.col("BRANCH") == branch)
        npl_count = len(bd.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))
        new_loans      = len(bd.filter(pl.col("ISSDTE") >= variables['PREPTDTE']))
        paid_2_or_less = len(bd.filter(pl.col("NOISTLPD") <= 2))

        branch_metrics.append({
            "BRANCH":          int(branch),
            "BRHCODE":         str(bd["BRHCODE"][0]) if len(bd) > 0 else "",
            "TOTAL_ACCOUNTS":  int(len(bd)),
            "TOTAL_BALANCE":   float(bd["BALANCE"].sum()),
            "NPL_COUNT":       int(npl_count),
            "NPL_PERCENTAGE":  float((npl_count / len(bd) * 100) if len(bd) > 0 else 0),
            "NEW_LOANS_COUNT": int(new_loans),
            "AVG_ARREAR":      float(bd["ARREAR2"].mean()),
            "PAID_2_OR_LESS":  int(paid_2_or_less),
            "PAYMENT_RATIO":   float((paid_2_or_less / len(bd) * 100) if len(bd) > 0 else 0),
        })

    branch_df = pl.DataFrame(branch_metrics, infer_schema_length=None) if branch_metrics else pl.DataFrame()
    write_text_report(branch_df, OUTPUT_PATH / "BRANCH_PERFORMANCE.txt", "BRANCH PERFORMANCE")
    print(f"\u2713 Branch performance analysis saved: {len(branch_df)} branches")

    print("\n10. Saving run variables...")
    # Variables frame: force all values to str for a stable schema
    variables_row = {
        k: (str(v) if not isinstance(v, (int, float, str)) else v)
        for k, v in variables.items()
    }
    write_text_report(pl.DataFrame([variables_row], infer_schema_length=None),
                      OUTPUT_PATH / "EIMAR301_VARIABLES.txt",
                      "EIMAR301 VARIABLES")

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total HP Direct loans processed: {len(filtered_loans)}")
    print(f"Report A (2+ months arrears):    {len(report_a_data)}")
    print(f"Report B (3-8 months arrears):   {len(report_b_data)}")
    print(f"Report C (New releases):         {len(report_c_data)}")
    print(f"Report D (2 installments paid):  {len(report_d_data)}")
    print(f"Previous month date:             {variables['PREPTDTE']}")
    print(f"Output saved to:                 {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
