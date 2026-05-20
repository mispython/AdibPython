from __future__ import annotations

import re
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# =========================
# CONFIGURATION
# =========================
BASE_INPUT  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")
USE_DUCKDB_COPY = False

# =========================
# UTILITIES
# =========================
def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if USE_DUCKDB_COPY:
        con = duckdb.connect()
        con.register("DF", df.to_arrow())
        con.execute(f"COPY DF TO '{path.as_posix()}' (FORMAT PARQUET)")
        con.close()
    else:
        pq.write_table(df.to_arrow(), path)

def write_csv(df: pl.DataFrame, path: Path):
    """Write DataFrame to CSV file with header"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)

def yyyymmdd_to_date(s: str) -> date:
    """Convert YYYYMMDD string to date"""
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_prev_month(d: date) -> date:
    """Get end of previous month — mirrors SAS INTNX('MONTH', d, -1, 'E')"""
    if d.month == 1:
        return date(d.year - 1, 12, 31)
    return date(d.year, d.month, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    """Convert date to MMYY string — mirrors SAS MMYYN4. format"""
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[date]:
    """Create date from components — mirrors SAS MDY()"""
    if None in (month, day, year):
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None

def read_first_line(path: Path) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.readline().strip()

# =========================
# FIELD SPECS
# SAS @position maps to Python [position-1 : position-1+length]
# =========================
FIELDS = [
    (0,   1,   'RECID',    str),
    (2,   12,  'MNIACTNO', str),
    (13,  23,  'LOANNOTE', str),
    (24,  74,  'NAME',     'u'),
    (75,  76,  'ACCTSTA',  'u'),
    (77,  82,  'PRODTYPE', str),
    (83,  84,  'PRSTCOND', 'u'),
    (85,  86,  'REGCARD',  'u'),
    (87,  88,  'IGNTKEY',  'u'),
    (89,  99,  'REPODIST', str),
    (100, 101, 'ACCTWOFF', 'u'),
    (102, 106, 'YY1',      int),
    (106, 108, 'MM1',      int),
    (108, 110, 'DD1',      int),
    (111, 112, 'MODEREPO', 'u'),
    (113, 117, 'YY2',      int),
    (117, 119, 'MM2',      int),
    (119, 121, 'DD2',      int),
    (122, 132, 'REPOPAID', str),
    (133, 139, 'REPOSTAT', 'u'),
    (140, 150, 'TKEPRICE', str),
    (151, 161, 'MRKTVAL',  str),
    (162, 172, 'RSVPRICE', str),
    (173, 183, 'FTHSCHLD', str),
    (184, 188, 'YY3',      int),
    (188, 190, 'MM3',      int),
    (190, 192, 'DD3',      int),
    (193, 194, 'MODEDISP', 'u'),
    (195, 205, 'APPVDISP', str),
    (206, 210, 'YY4',      int),
    (210, 212, 'MM4',      int),
    (212, 214, 'DD4',      int),
    (215, 219, 'YY5',      int),
    (219, 221, 'MM5',      int),
    (221, 223, 'DD5',      int),
    (224, 228, 'YY6',      int),
    (228, 230, 'MM6',      int),
    (230, 232, 'DD6',      int),
    (233, 243, 'HOPRICE',  str),
    (244, 249, 'NOAUCT',   str),
    (250, 270, 'PRIOUT',   str),
]

DATE_COLS = [
    ('YY1', 'MM1', 'DD1', 'DATEWOFF'),
    ('YY2', 'MM2', 'DD2', 'DATEREPO'),
    ('YY3', 'MM3', 'DD3', 'DATE5TH'),
    ('YY4', 'MM4', 'DD4', 'DATEAPRV'),
    ('YY5', 'MM5', 'DD5', 'DATESTLD'),
    ('YY6', 'MM6', 'DD6', 'DATEHO'),
]

DATE_COMPONENT_COLS = [
    'YY1','MM1','DD1','YY2','MM2','DD2',
    'YY3','MM3','DD3','YY4','MM4','DD4',
    'YY5','MM5','DD5','YY6','MM6','DD6',
]

# =========================
# DATA READING
# =========================
def read_rpvdata() -> pl.DataFrame:
    """
    Read RPVBDATA.txt with fixed-width parsing.
    Skips first line (header). Mirrors SAS FIRSTOBS=2 + INPUT statement.
    """
    with open(BASE_INPUT / "RPVBDATA.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]   # skip header line

    data = []
    for line in lines:
        line = line.rstrip('\n')
        if not line.strip():
            continue

        rec = {}
        for start, end, field, dtype in FIELDS:
            raw = line[start:end].strip() if len(line) >= end else ''
            if dtype == 'u':
                rec[field] = raw.upper()
            elif dtype == int:
                rec[field] = int(raw) if raw.isdigit() else None
            else:
                rec[field] = raw
        data.append(rec)

    df = pl.DataFrame(data)

    # Build date columns — mirrors SAS MDY(MM,DD,YY)
    # Use default-arg capture to avoid lambda closure bug
    for yy, mm, dd, dcol in DATE_COLS:
        df = df.with_columns(
            pl.struct([yy, mm, dd]).map_elements(
                lambda x, m=mm, d=dd, y=yy: mdy(x[m], x[d], x[y]),
                return_dtype=pl.Date
            ).alias(dcol)
        )

    # Drop raw date component columns — mirrors SAS DROP= option
    return df.drop([c for c in df.columns if c in DATE_COMPONENT_COLS])


# =========================
# MAIN PROCESSING
# =========================
def main():
    # -------------------------------------------------------
    # STEP 1: Read TBDATE from RPVBDATA — derive REPTDT / PREVDT
    # Mirrors SAS DATA REPTDATE step
    # -------------------------------------------------------
    print("=" * 60)
    print("STEP 1: Processing RPVBDATA dates")
    print("=" * 60)

    try:
        line = read_first_line(BASE_INPUT / "RPVBDATA.txt")
        tbdate_rpvb = line[2:10]   # @03 TBDATE $8. → 0-indexed [2:10]

        if not (tbdate_rpvb.isdigit() and len(tbdate_rpvb) == 8):
            raise ValueError(f"Invalid TBDATE: '{tbdate_rpvb}'")

        tb_date  = yyyymmdd_to_date(tbdate_rpvb)
        reptdate = end_of_prev_month(tb_date)       # INTNX('MONTH',...,-1,'E')
        prevdate = end_of_prev_month(reptdate)       # INTNX('MONTH',...,-1,'E')
        reptdt   = mmyy_format(reptdate)             # PUT(..., MMYYN4.)
        prevdt   = mmyy_format(prevdate)

        print(f"✓ TBDATE: {tbdate_rpvb}  →  REPTDT: {reptdt}, PREVDT: {prevdt}")

    except Exception as e:
        print(f"✗ Error: {e}")
        today    = date.today()
        reptdate = end_of_prev_month(today)
        prevdate = end_of_prev_month(reptdate)
        reptdt   = mmyy_format(reptdate)
        prevdt   = mmyy_format(prevdate)
        tbdate_rpvb = "N/A"
        print(f"  Fallback: REPTDT={reptdt}, PREVDT={prevdt}")

    # -------------------------------------------------------
    # STEP 2: Read TBDATE from SRSDATA — derive SRSTDT
    # Mirrors SAS DATA _NULL_ step (direct INPUT, no month shift)
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 2: Processing SRSDATA dates")
    print("=" * 60)

    tbdate_srs = "N/A"
    try:
        line       = read_first_line(BASE_INPUT / "SRSDATA.txt")
        tbdate_srs = line[0:8]   # @01 TBDATE $8.

        if tbdate_srs.isdigit() and len(tbdate_srs) == 8:
            srs_date = yyyymmdd_to_date(tbdate_srs)
            srstdt   = mmyy_format(srs_date)         # direct PUT, no INTNX
            print(f"✓ TBDATE: {tbdate_srs}  →  SRSTDT: {srstdt}")
        else:
            # Try extracting 8 consecutive digits as fallback
            match = re.search(r'(\d{8})', line)
            if match:
                srs_date = yyyymmdd_to_date(match.group(1))
                srstdt   = mmyy_format(srs_date)
                tbdate_srs = match.group(1)
                print(f"✓ Extracted date: {tbdate_srs}  →  SRSTDT: {srstdt}")
            else:
                srstdt = reptdt
                print(f"⚠ Could not parse SRSDATA date, using REPTDT fallback: {srstdt}")

    except Exception as e:
        print(f"✗ Error: {e}")
        srstdt = reptdt
        print(f"  Using REPTDT as fallback for SRSTDT: {srstdt}")

    # -------------------------------------------------------
    # STEP 3: Date guard — mirrors SAS %MACRO PROCESS / ABORT 77
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: Date validation guard")
    print("=" * 60)

    print(f"  REPTDT={reptdt}  vs  SRSTDT={srstdt}")
    if reptdt != srstdt:
        error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})"
        print(f"✗ {error_msg}")
        raise RuntimeError(error_msg)
    print(f"✓ Dates match — proceeding")

    # -------------------------------------------------------
    # STEP 4: Read RPVBDATA — mirrors SAS DATA RPVB1
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Reading RPVBDATA (fixed-width parse)")
    print("=" * 60)

    rpvb1 = read_rpvdata()
    print(f"✓ RPVB1: {len(rpvb1)} records, {len(rpvb1.columns)} columns")

    # -------------------------------------------------------
    # STEP 5: Filter — mirrors SAS DATA RPVB2 / RPVB3
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5: Filtering (RPVB2 and RPVB3)")
    print("=" * 60)

    if len(rpvb1) > 0 and 'ACCTSTA' in rpvb1.columns:
        # SAS: IF ACCTSTA IN ('D','S','R')
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        print(f"✓ RPVB2: {len(rpvb2)} records  (ACCTSTA in D, S, R)")

        # SAS: IF DATESTLD NE ''  (date not null)
        if 'DATESTLD' in rpvb2.columns:
            rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null())
            print(f"✓ RPVB3: {len(rpvb3)} records  (DATESTLD not null)")
        else:
            print("⚠ DATESTLD column missing — RPVB3 will be empty")
            rpvb3 = rpvb2.filter(pl.lit(False))
    else:
        print("⚠ No data or ACCTSTA column missing")
        rpvb2 = rpvb3 = rpvb1

    # -------------------------------------------------------
    # STEP 6: Build REPO.REPS&REPTDT = RPVB3 + REPO.REPS&PREVDT
    # Mirrors SAS DATA REPO.REPS&REPTDT; SET RPVB3 REPO.REPS&PREVDT
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 6: Creating REPO.REPS output")
    print("=" * 60)

    repo_prev_path  = BASE_OUTPUT / "REPO" / f"REPS_{prevdt}.parquet"
    repo_curr_path  = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.parquet"
    repo_curr_csv   = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.csv"

    print(f"  Previous: {repo_prev_path}")
    print(f"  Output  : {repo_curr_path}")
    print(f"  Output  : {repo_curr_csv}")

    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        print(f"✓ Loaded previous REPO: {len(repo_prev)} records")

        # Align schemas before concat
        if len(rpvb3) > 0 and len(repo_prev) > 0:
            all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
            for col in all_cols:
                if col not in rpvb3.columns:
                    rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
                if col not in repo_prev.columns:
                    repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
            rpvb3     = rpvb3.select(all_cols)
            repo_prev = repo_prev.select(all_cols)

    except Exception as e:
        print(f"  No previous REPO file found ({e}) — using RPVB3 only")
        repo_prev = pl.DataFrame()

    repo_reps = (
        rpvb3 if len(repo_prev) == 0
        else pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
    )

    write_parquet(repo_reps, repo_curr_path)
    write_csv(repo_reps, repo_curr_csv)
    print(f"✓ REPO saved: {len(repo_reps)} records  →  parquet + csv")

    # -------------------------------------------------------
    # STEP 7: Build REPOWH.REPS&REPTDT — mirrors SAS PROC SORT NODUPKEY BY MNIACTNO
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 7: Creating REPOWH.REPS output (NODUPKEY)")
    print("=" * 60)

    repowh_path = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.parquet"
    repowh_csv  = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.csv"

    if len(repo_reps) > 0 and 'MNIACTNO' in repo_reps.columns:
        repowh_reps       = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
        duplicates_removed = len(repo_reps) - len(repowh_reps)
        print(f"✓ Duplicates removed: {duplicates_removed}")
    else:
        repowh_reps = repo_reps
        print("  No MNIACTNO column or empty data — skipping deduplication")

    write_parquet(repowh_reps, repowh_path)
    write_csv(repowh_reps, repowh_csv)
    print(f"✓ REPOWH saved: {len(repowh_reps)} records  →  parquet + csv")

    # -------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  TBDATE (RPVBDATA) : {tbdate_rpvb}")
    print(f"  TBDATE (SRSDATA)  : {tbdate_srs}")
    print(f"  REPTDT            : {reptdt}")
    print(f"  PREVDT            : {prevdt}")
    print(f"  SRSTDT            : {srstdt}")
    print(f"  RPVB1             : {len(rpvb1)} records")
    print(f"  RPVB2             : {len(rpvb2)} records")
    print(f"  RPVB3             : {len(rpvb3)} records")
    print(f"  REPO_REPS         : {len(repo_reps)} records")
    print(f"  REPOWH_REPS       : {len(repowh_reps)} records")
    print("\nOutput files:")
    print(f"  {repo_curr_path}")
    print(f"  {repo_curr_csv}")
    print(f"  {repowh_path}")
    print(f"  {repowh_csv}")
    print("=" * 60)
    print("✓ Processing completed successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()
