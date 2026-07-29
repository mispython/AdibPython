"""
EIBWHP02 - Python translation of the original SAS job.

Sector classification now uses the real $SECTA./$SECTB. logic, ported from
the PBBLNFMT SAS format library into pbblnfmt.py (format_secta/format_sectb).
Make sure pbblnfmt.py sits next to this file (or is on PYTHONPATH).

Note on REPTDATE: rather than reading the BNM.REPTDATE control dataset,
this uses yesterday's date (datetime.now() - timedelta(days=1)), same as
the original standalone Python script. See get_reptdate().

Everything else follows the SAS DATA step / PROC SUMMARY logic line-for-line
as closely as pandas allows. Comments reference the original SAS statements
so you can cross-check.
"""

import os
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
import pandas as pd
import pyreadstat

from pbblnfmt import format_secta, format_sectb

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP02"
SPOOL_DIR = BASE_DIR / "output/EIBWHP02"
JOB_NAME = "EIBWHP02"

# Columns actually needed from each input, mirroring the KEEP= lists in SAS.
ALW1_COLS = ["ACCTNO", "NOTENO", "SECTORCD", "PRODUCT", "NOTETERM",
             "BALANCE", "PRODCD", "CUSTCD", "AMTIND", "ISSDTE", "BRANCH"]

ALW0_COLS = ["ACCTNO", "NOTENO", "SECTORCD", "PRODUCT", "NOTETERM",
             "EARNTERM", "BALANCE", "APPRDATE", "APPRLIM2", "PRODCD",
             "CUSTCD", "AMTIND", "ISSDTE", "BRANCH"]

# Columns pulled from the ULOAN dataset. Confirmed against real data:
# ULOAN only carries SECTORCD, AMTIND, CUSTCD, BRANCH. DISBURSE, REPAID,
# and APPRLIM2 do not exist on this input at all -- in the original SAS,
# DISBURSE/REPAID are defaulted to 0 via RETAIN, and APPRLIM2 is left
# uninitialized (missing, which sums as 0 downstream). See build_ualw().
UALW_COLS = ["SECTORCD", "AMTIND", "CUSTCD", "BRANCH"]

PRODUCT_FILTER = [131, 132, 720, 725]
PRODCD_PREFIX_FILTER = ("341", "342", "343", "344")
EXCLUDED_SECTCD = "0210"
SMI_CUSTCD = ["66", "67", "68", "69"]

NUM_READ_PROCESSES = int(os.environ.get("SAS_READ_PROCESSES", 4))

# =====================================================
# PERIOD CALCULATION  (mirrors the first DATA step)
# =====================================================

def compute_period_vars(reptdate: date) -> dict:
    """
    Reproduces:
      SELECT(DAY(REPTDATE)) -> SDD/WK/WK1
      MM = MONTH(REPTDATE); WK1 handling for MM1
      SDATE = MDY(MM,SDD,YEAR(REPTDATE))
    """
    day = reptdate.day

    if day == 8:
        sdd, wk, wk1 = 1, "1", "4"
    elif day == 15:
        sdd, wk, wk1 = 9, "2", "1"
    elif day == 22:
        sdd, wk, wk1 = 16, "3", "2"
    else:
        sdd, wk, wk1 = 23, "4", "3"

    mm = reptdate.month

    if wk == "1":
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # NOTE: like the original SAS, this does not adjust the year when
    # MM1 wraps from January back to December. Preserved as-is.
    sdate = date(reptdate.year, mm, sdd)

    return {
        "NOWK": wk,
        "NOWK1": wk1,
        "REPTMON": f"{mm:02d}",
        "REPTMON1": f"{mm1:02d}",
        "REPTYEAR": str(reptdate.year),
        "REPTDAY": f"{day:02d}",
        "RDATE": reptdate.strftime("%d/%m/%y"),
        "SDATE": sdate.strftime("%d/%m/%y"),
    }


def get_reptdate() -> date:
    """
    Uses yesterday's date instead of reading the BNM.REPTDATE control
    dataset (matches the date logic from the original standalone Python
    script: datetime.now() - timedelta(days=1)).
    """
    return (datetime.now() - timedelta(days=1)).date()

# =====================================================
# DISP SIMULATION
# =====================================================

def disp_shr(dataset_path: Path):
    if not dataset_path.exists():
        raise FileNotFoundError(f"[DISP ERROR] Required dataset missing: {dataset_path}")

# =====================================================
# DATA READING
# =====================================================

def read_sas_dataset(file_path: Path, usecols=None, num_processes=1):
    try:
        if num_processes and num_processes > 1:
            # Newer pyreadstat versions don't accept num_processes/multiprocess
            # directly on read_sas7bdat() -- multiprocessing goes through the
            # separate read_file_multiprocessing() wrapper instead.
            df, meta = pyreadstat.read_file_multiprocessing(
                pyreadstat.read_sas7bdat,
                str(file_path),
                usecols=usecols,
                num_processes=num_processes,
            )
        else:
            df, meta = pyreadstat.read_sas7bdat(str(file_path), usecols=usecols)

        print(f"[READ] {file_path.name}: {len(df)} rows, {len(df.columns)} cols "
              f"(usecols={usecols}, num_processes={num_processes})")

        if usecols:
            missing = [c for c in usecols if c not in df.columns]
            if missing:
                print(f"[WARN] {file_path.name}: requested column(s) not found and "
                      f"silently dropped by pyreadstat: {missing}. "
                      f"Check for a naming mismatch (case, abbreviation, etc). "
                      f"Actual columns present: {list(df.columns)}")
        return df
    except TypeError as e:
        # Fallback for pyreadstat versions that support neither API variant
        # for multiprocessing -- just read single-process rather than fail.
        print(f"[WARN] Multiprocessing read unavailable ({e}); falling back to single-process read.")
        df, meta = pyreadstat.read_sas7bdat(str(file_path), usecols=usecols)
        print(f"[READ] {file_path.name}: {len(df)} rows, {len(df.columns)} cols "
              f"(usecols={usecols}, num_processes=1 [fallback])")
        if usecols:
            missing = [c for c in usecols if c not in df.columns]
            if missing:
                print(f"[WARN] {file_path.name}: requested column(s) not found and "
                      f"silently dropped by pyreadstat: {missing}. "
                      f"Actual columns present: {list(df.columns)}")
        return df
    except Exception as e:
        raise Exception(f"Error reading {file_path}: {e}")

# =====================================================
# SECTA / SECTB DUAL-FORMAT EXPANSION
#   PUT(SECTORCD,$SECTA.) -> if non-blank, OUTPUT
#   PUT(SECTORCD,$SECTB.) -> if non-blank, OUTPUT (again)
# A single input row can therefore produce 0, 1, or 2 output rows.
#
# format_secta/format_sectb do string RANGE comparisons on 4-character
# zero-padded sector codes (e.g. '0110' <= c <= '0139'). If SECTORCD comes
# through as a number (e.g. 110 instead of '0110'), naive str() gives
# "110" and every range comparison silently breaks -- so raw values are
# normalized to zero-padded strings before being handed to the format
# functions.
# =====================================================

def _sectorcd_to_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float):
        if pd.isna(val):
            return ""
        if val.is_integer():
            return str(int(val)).zfill(4)
        return str(val).strip()
    if isinstance(val, int):
        return str(val).zfill(4)
    s = str(val).strip()
    # A purely numeric string (e.g. '110' instead of '0110') needs the same
    # zero-padding as the int/float cases above -- SAS character columns
    # aren't guaranteed to already carry leading zeros.
    if s.isdigit():
        return s.zfill(4)
    return s


def expand_sector_formats(df: pd.DataFrame, sector_col="SECTORCD") -> pd.DataFrame:
    normalized = df[sector_col].apply(_sectorcd_to_str)

    frames = []
    for fmt_func in (format_secta, format_sectb):
        tmp = df.copy()
        tmp["SECTCD"] = normalized.apply(fmt_func)
        tmp["SECTCD"] = tmp["SECTCD"].str.strip()
        tmp = tmp[tmp["SECTCD"] != ""]
        frames.append(tmp)

    if not frames or all(f.empty for f in frames):
        return pd.DataFrame(columns=list(df.columns) + ["SECTCD"])
    return pd.concat(frames, ignore_index=True)

# =====================================================
# ALW  (merge of prior-period ALW1 and current-period ALW)
# =====================================================

def build_alw(loan_prev_path: Path, loan_curr_path: Path) -> pd.DataFrame:
    # PROC SORT ... OUT=ALW1 (prior period, PRODUCT filter, then renamed on merge)
    alw1_raw = read_sas_dataset(loan_prev_path, usecols=ALW1_COLS, num_processes=NUM_READ_PROCESSES)
    print(f"[DEBUG] alw1 (prior period) raw rows: {len(alw1_raw)}; "
          f"PRODUCT sample: {alw1_raw['PRODUCT'].dropna().unique()[:10].tolist()}")
    alw1 = alw1_raw[alw1_raw["PRODUCT"].isin(PRODUCT_FILTER)].copy()
    print(f"[DEBUG] alw1 after PRODUCT filter {PRODUCT_FILTER}: {len(alw1)} rows")
    alw1 = alw1.rename(columns={"BALANCE": "LASTBAL", "NOTETERM": "LASTNOTE"})
    alw1 = alw1.sort_values(["ACCTNO", "NOTENO", "SECTORCD"])

    # PROC SORT ... OUT=ALW (current period, PRODUCT filter)
    alw0_raw = read_sas_dataset(loan_curr_path, usecols=ALW0_COLS, num_processes=NUM_READ_PROCESSES)
    print(f"[DEBUG] alw0 (current period) raw rows: {len(alw0_raw)}; "
          f"PRODUCT sample: {alw0_raw['PRODUCT'].dropna().unique()[:10].tolist()}")
    alw0 = alw0_raw[alw0_raw["PRODUCT"].isin(PRODUCT_FILTER)].copy()
    print(f"[DEBUG] alw0 after PRODUCT filter {PRODUCT_FILTER}: {len(alw0)} rows")
    alw0 = alw0.sort_values(["ACCTNO", "NOTENO", "SECTORCD"])

    # MERGE ALW1(IN=A) ALW(IN=B); BY ACCTNO NOTENO SECTORCD;
    merged = pd.merge(
        alw1, alw0,
        on=["ACCTNO", "NOTENO", "SECTORCD"],
        how="outer", suffixes=("_prev", "_curr"), indicator=True,
    )
    print(f"[DEBUG] merged rows: {len(merged)}; "
          f"_merge counts: {merged['_merge'].value_counts().to_dict()}")

    # Shared columns: in a SAS MERGE, the later dataset's (ALW / current
    # period) value wins whenever that BY-group is present in it; otherwise
    # fall back to the prior period's value.
    #
    # NOTE: pandas only suffixes a column with _prev/_curr when that column
    # name exists in BOTH input frames. If one side is missing a requested
    # column entirely (see the [WARN] at read time), it shows up here
    # unsuffixed instead -- handled below rather than assumed away.
    shared_cols = ["PRODUCT", "PRODCD", "CUSTCD", "AMTIND", "ISSDTE", "BRANCH"]
    for col in shared_cols:
        prev_col = f"{col}_prev"
        curr_col = f"{col}_curr"
        has_prev = prev_col in merged.columns
        has_curr = curr_col in merged.columns

        if has_prev and has_curr:
            merged[col] = merged[curr_col].combine_first(merged[prev_col])
            merged = merged.drop(columns=[curr_col, prev_col])
        elif col in merged.columns:
            # Only one of the two frames had this column, so pandas never
            # suffixed it -- already in its final form, nothing to do.
            pass
        elif has_curr:
            merged[col] = merged[curr_col]
            merged = merged.drop(columns=[curr_col])
        elif has_prev:
            merged[col] = merged[prev_col]
            merged = merged.drop(columns=[prev_col])
        else:
            print(f"[WARN] Column '{col}' not present in either period's data; filling with NaN.")
            merged[col] = pd.NA

    both = merged["_merge"] == "both"
    left_only = merged["_merge"] == "left_only"    # in ALW1 only (^B)
    right_only = merged["_merge"] == "right_only"  # in ALW only (^A)

    merged["NOACCT"] = 1
    merged["DISBURSE"] = 0.0
    merged["REPAID"] = 0.0

    repaid_mask = both & (merged["LASTBAL"] > merged["BALANCE"])
    disburse_mask = both & ~repaid_mask
    merged.loc[repaid_mask, "REPAID"] = merged.loc[repaid_mask, "LASTBAL"] - merged.loc[repaid_mask, "BALANCE"]
    merged.loc[disburse_mask, "DISBURSE"] = merged.loc[disburse_mask, "BALANCE"] - merged.loc[disburse_mask, "LASTBAL"]

    merged.loc[left_only, "REPAID"] = merged.loc[left_only, "LASTBAL"]
    merged.loc[right_only, "DISBURSE"] = merged.loc[right_only, "BALANCE"]

    print(f"[DEBUG] merged SECTORCD sample (raw): {merged['SECTORCD'].dropna().unique()[:10].tolist()}")
    print(f"[DEBUG] merged SECTORCD normalized sample: "
          f"{merged['SECTORCD'].apply(_sectorcd_to_str).unique()[:10].tolist()}")

    expanded = expand_sector_formats(merged, sector_col="SECTORCD")
    print(f"[DEBUG] alw after SECTA/SECTB expansion: {len(expanded)} rows "
          f"(from {len(merged)} pre-expansion rows)")
    if len(expanded) > 0:
        print(f"[DEBUG] alw SECTCD value counts (top 10): "
              f"{expanded['SECTCD'].value_counts().head(10).to_dict()}")

    keep = ["SECTCD", "DISBURSE", "REPAID", "APPRLIM2", "AMTIND",
            "CUSTCD", "NOACCT", "PRODCD", "BRANCH"]
    return expanded[keep]

# =====================================================
# UALW  (from ULOAN, appended onto ALW)
# =====================================================

def build_ualw(uloan_path: Path) -> pd.DataFrame:
    df = read_sas_dataset(uloan_path, usecols=UALW_COLS, num_processes=NUM_READ_PROCESSES)
    print(f"[DEBUG] uloan raw rows: {len(df)}; "
          f"SECTORCD sample: {df['SECTORCD'].dropna().unique()[:10].tolist()}")

    # RETAIN DISBURSE REPAID 0; -> ULOAN has no DISBURSE/REPAID columns at
    # all, so SAS defaults every row to 0 for both. Confirmed against the
    # real file (see UALW_COLS comment).
    df["DISBURSE"] = 0.0
    df["REPAID"] = 0.0

    # APPRLIM2 isn't on ULOAN and isn't RETAINed either, so in the original
    # SAS every row comes out missing (.) for it. PROC SUMMARY treats
    # missing as 0 when summing, so NaN here reproduces that behavior via
    # pandas' default sum-skips-NaN.
    df["APPRLIM2"] = pd.NA

    expanded = expand_sector_formats(df, sector_col="SECTORCD")
    print(f"[DEBUG] ualw after SECTA/SECTB expansion: {len(expanded)} rows "
          f"(from {len(df)} pre-expansion rows)")

    keep = ["SECTCD", "DISBURSE", "REPAID", "APPRLIM2", "AMTIND", "CUSTCD", "BRANCH"]
    return expanded[keep]

# =====================================================
# SUMMARIES
#   PROC SUMMARY DATA=ALW NWAY CLASS BRANCH CUSTCD AMTIND ...
#     WHERE SUBSTR(PRODCD,1,3) IN ('341'..'344') AND SECTCD NE '0210'
#   PROC SUMMARY DATA=ALWX NWAY WHERE CUSTCD IN ('66'..'69') CLASS BRANCH
# =====================================================

def summarize(alw: pd.DataFrame) -> pd.DataFrame:
    print(f"[DEBUG] combined (alw+ualw) rows entering summarize: {len(alw)}")
    if len(alw) > 0:
        print(f"[DEBUG] combined PRODCD sample: {alw['PRODCD'].dropna().unique()[:10].tolist()}")
        print(f"[DEBUG] combined CUSTCD sample: {alw['CUSTCD'].dropna().unique()[:15].tolist()}")

    # NOTE: rows appended from UALW have no PRODCD/NOACCT (NaN), so
    # SUBSTR(PRODCD,1,3) never matches the filter and they're excluded here
    # -- same outcome as the original SAS WHERE clause on missing PRODCD.
    prodcd_prefix = alw["PRODCD"].astype(str).str[:3]
    print(f"[DEBUG] PRODCD prefix value counts (top 10): {prodcd_prefix.value_counts().head(10).to_dict()}")
    mask = prodcd_prefix.isin(PRODCD_PREFIX_FILTER) & (alw["SECTCD"] != EXCLUDED_SECTCD)
    filtered = alw[mask]
    print(f"[DEBUG] rows after PRODCD prefix {PRODCD_PREFIX_FILTER} + SECTCD!={EXCLUDED_SECTCD} filter: {len(filtered)}")

    alwx = (
        filtered.groupby(["BRANCH", "CUSTCD", "AMTIND"], as_index=False)
        [["DISBURSE", "REPAID", "APPRLIM2", "NOACCT"]]
        .sum()
    )
    print(f"[DEBUG] alwx (first summary) rows: {len(alwx)}")
    if len(alwx) > 0:
        print(f"[DEBUG] alwx CUSTCD sample: {alwx['CUSTCD'].astype(str).unique()[:15].tolist()}")

    custcd_norm = alwx["CUSTCD"].astype(str).str.strip()
    smi_mask = custcd_norm.isin(SMI_CUSTCD)
    print(f"[DEBUG] rows matching SMI_CUSTCD {SMI_CUSTCD}: {smi_mask.sum()} of {len(alwx)}")

    alwloan = (
        alwx[smi_mask]
        .groupby("BRANCH", as_index=False)["DISBURSE"]
        .sum()
        .sort_values("BRANCH")
    )
    return alwloan

# =====================================================
# REPORT / SYSOUT
# =====================================================

def build_report(alwloan: pd.DataFrame, rdate_str: str) -> list:
    lines = [
        f"{JOB_NAME}: SMI (CUSTCD 66,67,68,69) BY BRANCH AS AT {rdate_str}",
        "FOR LOANS PRODUCTS 131,132,720,725",
        "-" * 40,
        f"{'BRANCH':<15}{'DISBURSE':>20}",
        "-" * 40,
    ]
    total = 0.0
    for _, row in alwloan.iterrows():
        branch = str(row["BRANCH"]).zfill(3)
        disburse = row["DISBURSE"]
        lines.append(f"{branch:<15}{disburse:>20,.2f}")
        total += disburse
    lines.append("-" * 40)
    lines.append(f"{'TOTAL':<15}{total:>20,.2f}")
    lines.append("-" * 40)
    lines.append("END OF REPORT")
    return lines


def write_sysout(records, spool_file: Path):
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    with open(spool_file, "w", encoding="utf-8") as f:
        for line in records:
            f.write(line + "\n")
    print(f"[SYSOUT] Report written to spool: {spool_file}")

# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    from datetime import datetime as dt

    print(f"========== START JOB {JOB_NAME} ==========")

    reptdate = get_reptdate()
    period = compute_period_vars(reptdate)
    print(f"[INFO] REPTDATE={reptdate} -> {period}")

    loan_prev_path = INPUT_DIR / f"loan{period['REPTMON1']}{period['NOWK1']}.sas7bdat"
    loan_curr_path = INPUT_DIR / f"loan{period['REPTMON']}{period['NOWK']}.sas7bdat"
    uloan_curr_path = INPUT_DIR / f"uloan{period['REPTMON']}{period['NOWK']}.sas7bdat"

    for path in (loan_prev_path, loan_curr_path, uloan_curr_path):
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {path.name}")

    alw = build_alw(loan_prev_path, loan_curr_path)
    ualw = build_ualw(uloan_curr_path)
    combined = pd.concat([alw, ualw], ignore_index=True)

    alwloan = summarize(combined)

    spool_file = SPOOL_DIR / f"{JOB_NAME}_{dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report = build_report(alwloan, period["RDATE"])
    write_sysout(report, spool_file)

    print(f"========== END JOB {JOB_NAME} ==========")

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)
