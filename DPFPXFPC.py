from __future__ import annotations

import re
import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

# =========================
# CONFIGURATION
# =========================
BASE_INPUT  = Path("/stgsrcsys/host/uat")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")
SRS_INPUT   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/SRSDATA.txt")
SAS_EPOCH   = date(1960, 1, 1)

# =========================
# UTILITIES
# =========================
def to_sas_date(d: date) -> int:
    return (d - SAS_EPOCH).days

def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_prev_month(d: date) -> date:
    return date(d.year - 1, 12, 31) if d.month == 1 else date(d.year, d.month, 1) - timedelta(days=1)

def mmyy_format(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[int]:
    if None in (month, day, year):
        return None
    try:
        return to_sas_date(date(year, month, day))
    except ValueError:
        return None

def read_first_line(path: Path) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.readline().strip()

def write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path)

def write_csv(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(path)

def clean_int_str(val: str) -> Optional[int]:
    """Convert string to int, handling scientific notation e.g. '8E+09' -> 8000000000"""
    if not val or not val.strip():
        return None
    try:
        return int(float(val.strip()))
    except (ValueError, OverflowError):
        return None

# =========================
# FIELD SPECS
# =========================
FIELDS = [
    (0,   1,   'RECID',    str),
    (2,   12,  'MNIACTNO', 'int_str'),   # force clean integer — prevents 8E+09
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
    ('YY1','MM1','DD1','DATEWOFF'),
    ('YY2','MM2','DD2','DATEREPO'),
    ('YY3','MM3','DD3','DATE5TH'),
    ('YY4','MM4','DD4','DATEAPRV'),
    ('YY5','MM5','DD5','DATESTLD'),
    ('YY6','MM6','DD6','DATEHO'),
]

DATE_DROP = {f"{p}{i}" for i in range(1,7) for p in ('YY','MM','DD')}

# =========================
# READ RPVB_TEXT.txt
# =========================
def read_rpvdata() -> pl.DataFrame:
    with open(BASE_INPUT / "RPVB_TEXT.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]   # skip header (FIRSTOBS=2)

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
            elif dtype == 'int_str':
                rec[field] = clean_int_str(raw)   # Int64, no scientific notation
            else:
                rec[field] = raw
        data.append(rec)

    df = pl.DataFrame(data)

    # Build SAS serial date columns (mirrors SAS MDY)
    for yy, mm, dd, dcol in DATE_COLS:
        df = df.with_columns(
            pl.struct([yy, mm, dd]).map_elements(
                lambda x, m=mm, d=dd, y=yy: mdy(x[m], x[d], x[y]),
                return_dtype=pl.Int32
            ).alias(dcol)
        )

    return df.drop([c for c in df.columns if c in DATE_DROP])

# =========================
# MAIN
# =========================
def main():
    sep = "=" * 60

    # ----------------------------------------------------------
    # STEP 1: RPVBDATA dates — mirrors SAS DATA REPTDATE
    # ----------------------------------------------------------
    print(f"{sep}\nSTEP 1: Processing RPVBDATA dates\n{sep}")
    tbdate_rpvb = reptdate_sas = prevdate_sas = "N/A"
    try:
        line        = read_first_line(BASE_INPUT / "RPVB_TEXT.txt")
        tbdate_rpvb = line[2:10]
        if not (tbdate_rpvb.isdigit() and len(tbdate_rpvb) == 8):
            raise ValueError(f"Invalid TBDATE: '{tbdate_rpvb}'")

        tb_date      = yyyymmdd_to_date(tbdate_rpvb)
        reptdate     = end_of_prev_month(tb_date)
        prevdate     = end_of_prev_month(reptdate)
        reptdt       = mmyy_format(reptdate)
        prevdt       = mmyy_format(prevdate)
        reptdate_sas = to_sas_date(reptdate)
        prevdate_sas = to_sas_date(prevdate)

        print(f"✓ TBDATE    : {tbdate_rpvb}")
        print(f"  REPTDATE  : {reptdate}  (SAS: {reptdate_sas})")
        print(f"  PREVDATE  : {prevdate}  (SAS: {prevdate_sas})")
        print(f"  REPTDT    : {reptdt} | PREVDT: {prevdt}")

    except Exception as e:
        print(f"✗ Error: {e} — using today as fallback")
        reptdate     = end_of_prev_month(date.today())
        prevdate     = end_of_prev_month(reptdate)
        reptdt       = mmyy_format(reptdate)
        prevdt       = mmyy_format(prevdate)
        reptdate_sas = to_sas_date(reptdate)
        prevdate_sas = to_sas_date(prevdate)

    # ----------------------------------------------------------
    # STEP 2: SRSDATA dates — mirrors SAS DATA _NULL_
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 2: Processing SRSDATA dates\n{sep}")
    tbdate_srs = "N/A"
    srstdt     = reptdt
    try:
        line       = read_first_line(SRS_INPUT)
        tbdate_srs = line[0:8]
        if not (tbdate_srs.isdigit() and len(tbdate_srs) == 8):
            m = re.search(r'(\d{8})', line)
            tbdate_srs = m.group(1) if m else None
        if tbdate_srs:
            srs_date = yyyymmdd_to_date(tbdate_srs)
            srstdt   = mmyy_format(srs_date)
            print(f"✓ TBDATE    : {tbdate_srs}  →  SRSTDT: {srstdt}  (SAS: {to_sas_date(srs_date)})")
        else:
            print(f"⚠ Could not parse SRSDATA date — fallback SRSTDT: {srstdt}")
    except Exception as e:
        print(f"✗ Error: {e} — fallback SRSTDT: {srstdt}")

    # ----------------------------------------------------------
    # STEP 3: Date guard — mirrors SAS %MACRO PROCESS / ABORT 77
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 3: Date validation guard\n{sep}")
    print(f"  REPTDT={reptdt}  vs  SRSTDT={srstdt}")
    if reptdt != srstdt:
        raise RuntimeError(f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})")
    print("✓ Dates match — proceeding")

    # ----------------------------------------------------------
    # STEP 4: Read RPVB_TEXT.txt — mirrors SAS DATA RPVB1
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 4: Reading RPVB_TEXT.txt\n{sep}")
    rpvb1 = read_rpvdata()
    print(f"✓ RPVB1  : {len(rpvb1):,} records  |  MNIACTNO dtype: {rpvb1['MNIACTNO'].dtype}")
    if len(rpvb1) > 0:
        print(f"  MNIACTNO sample : {rpvb1['MNIACTNO'].drop_nulls().head(3).to_list()}")

    # ----------------------------------------------------------
    # STEP 5: Filter — mirrors SAS DATA RPVB2 / RPVB3
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 5: Filtering\n{sep}")
    rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D","S","R"])) if "ACCTSTA" in rpvb1.columns else rpvb1
    rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null()) if "DATESTLD" in rpvb2.columns else rpvb2.filter(pl.lit(False))
    print(f"✓ RPVB2  : {len(rpvb2):,} records  (ACCTSTA in D,S,R)")
    print(f"✓ RPVB3  : {len(rpvb3):,} records  (DATESTLD not null)")

    # ----------------------------------------------------------
    # STEP 6: REPO.REPS — mirrors SAS SET RPVB3 REPO.REPS&PREVDT
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 6: Creating REPO.REPS\n{sep}")
    repo_prev_path = BASE_OUTPUT / "REPO" / f"REPS_{prevdt}.parquet"
    repo_curr_path = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.parquet"
    repo_curr_csv  = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.csv"

    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        # Ensure MNIACTNO stays Int64 after loading from parquet
        if 'MNIACTNO' in repo_prev.columns:
            repo_prev = repo_prev.with_columns(pl.col('MNIACTNO').cast(pl.Int64, strict=False))
        # Align schemas
        all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
        for col in all_cols:
            if col not in rpvb3.columns:
                rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
            if col not in repo_prev.columns:
                repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
        rpvb3     = rpvb3.select(all_cols)
        repo_prev = repo_prev.select(all_cols)
        repo_reps = pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
        print(f"✓ Loaded previous: {len(repo_prev):,} records")
    except Exception as e:
        print(f"  No previous file ({e}) — using RPVB3 only")
        repo_reps = rpvb3

    write_parquet(repo_reps, repo_curr_path)
    write_csv(repo_reps, repo_curr_csv)
    print(f"✓ REPO saved : {len(repo_reps):,} records  →  {repo_curr_path.name} + .csv")

    # ----------------------------------------------------------
    # STEP 7: REPOWH — mirrors SAS PROC SORT NODUPKEY BY MNIACTNO
    # ----------------------------------------------------------
    print(f"\n{sep}\nSTEP 7: Creating REPOWH.REPS (NODUPKEY)\n{sep}")
    repowh_path = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.parquet"
    repowh_csv  = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.csv"

    repowh_reps = (
        repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
        if "MNIACTNO" in repo_reps.columns and len(repo_reps) > 0
        else repo_reps
    )
    print(f"✓ Duplicates removed : {len(repo_reps) - len(repowh_reps):,}")

    write_parquet(repowh_reps, repowh_path)
    write_csv(repowh_reps, repowh_csv)
    print(f"✓ REPOWH saved: {len(repowh_reps):,} records  →  {repowh_path.name} + .csv")

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    print(f"\n{sep}\nSUMMARY\n{sep}")
    print(f"  TBDATE RPVB  : {tbdate_rpvb}  |  TBDATE SRS : {tbdate_srs}")
    print(f"  REPTDATE     : {reptdate}  (SAS: {reptdate_sas})")
    print(f"  PREVDATE     : {prevdate}  (SAS: {prevdate_sas})")
    print(f"  REPTDT       : {reptdt}  |  PREVDT: {prevdt}  |  SRSTDT: {srstdt}")
    print(f"  RPVB1/2/3    : {len(rpvb1):,} / {len(rpvb2):,} / {len(rpvb3):,}")
    print(f"  REPO / REPOWH: {len(repo_reps):,} / {len(repowh_reps):,}")
    print(f"\n  Output files:")
    print(f"    {repo_curr_path}")
    print(f"    {repo_curr_csv}")
    print(f"    {repowh_path}")
    print(f"    {repowh_csv}")
    print(f"{sep}\n✓ Processing completed successfully\n{sep}")


if __name__ == "__main__":
    main()
