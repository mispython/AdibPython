from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
import re

# =========================
# CONFIGURATION
# =========================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
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

def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_prev_month(d: date) -> date:
    return date(d.year, d.month, 1) - timedelta(days=1) if d.month > 1 else date(d.year - 1, 12, 31)

def mmyy_format(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"

def mdy(month: int, day: int, year: int) -> Optional[date]:
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
# =========================
FIELDS = [
    (0,1,'RECID',str), (2,12,'MNIACTNO',str), (13,23,'LOANNOTE',str), (24,74,'NAME','u'),
    (75,76,'ACCTSTA','u'), (77,82,'PRODTYPE',str), (83,84,'PRSTCOND','u'), (85,86,'REGCARD','u'),
    (87,88,'IGNTKEY','u'), (89,99,'REPODIST',str), (100,101,'ACCTWOFF','u'), (102,106,'YY1',int),
    (106,108,'MM1',int), (108,110,'DD1',int), (111,112,'MODEREPO','u'), (113,117,'YY2',int),
    (117,119,'MM2',int), (119,121,'DD2',int), (122,132,'REPOPAID',str), (133,139,'REPOSTAT','u'),
    (140,150,'TKEPRICE',str), (151,161,'MRKTVAL',str), (162,172,'RSVPRICE',str), (173,183,'FTHSCHLD',str),
    (184,188,'YY3',int), (188,190,'MM3',int), (190,192,'DD3',int), (193,194,'MODEDISP','u'),
    (195,205,'APPVDISP',str), (206,210,'YY4',int), (210,212,'MM4',int), (212,214,'DD4',int),
    (215,219,'YY5',int), (219,221,'MM5',int), (221,223,'DD5',int), (224,228,'YY6',int),
    (228,230,'MM6',int), (230,232,'DD6',int), (233,243,'HOPRICE',str), (244,249,'NOAUCT',str),
    (250,270,'PRIOUT',str)
]

DATES = [('YY1','MM1','DD1','DATEWOFF'), ('YY2','MM2','DD2','DATEREPO'), ('YY3','MM3','DD3','DATE5TH'),
         ('YY4','MM4','DD4','DATEAPRV'), ('YY5','MM5','DD5','DATESTLD'), ('YY6','MM6','DD6','DATEHO')]

# =========================
# DATA READING
# =========================
def read_rpvdata() -> pl.DataFrame:
    with open(BASE_INPUT / "RPVBDATA.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]
    
    data = []
    for line in lines:
        line = line.rstrip('\n')
        if not line.strip():
            continue
        
        rec = {}
        for start, end, field, dtype in FIELDS:
            val = line[start:end].strip() if len(line) >= end else ''
            rec[field] = val.upper() if dtype == 'u' else (int(val) if dtype == int and val.isdigit() else val if dtype == str else None)
        data.append(rec)
    
    df = pl.DataFrame(data)
    
    for yy, mm, dd, dcol in DATES:
        df = df.with_columns(pl.struct([yy,mm,dd]).map_elements(
            lambda x: mdy(x[mm], x[dd], x[yy]), return_dtype=pl.Date).alias(dcol))
    
    return df.drop([c for c in df.columns if any(c == x for x in 
        ['YY1','MM1','DD1','YY2','MM2','DD2','YY3','MM3','DD3','YY4','MM4','DD4','YY5','MM5','DD5','YY6','MM6','DD6'])])

# =========================
# MAIN PROCESSING
# =========================
def main():
    print("=" * 60 + "\nProcessing RPVBDATA dates\n" + "=" * 60)
    
    try:
        line = read_first_line(BASE_INPUT / "RPVBDATA.txt")
        tbdate_rpvb = line[2:10]
        
        if not (tbdate_rpvb.isdigit() and len(tbdate_rpvb) == 8):
            raise ValueError(f"Invalid TBDATE: {tbdate_rpvb}")
        
        tb_date = yyyymmdd_to_date(tbdate_rpvb)
        reptdate = end_of_prev_month(tb_date)
        prevdate = end_of_prev_month(reptdate)
        reptdt, prevdt = mmyy_format(reptdate), mmyy_format(prevdate)
        
        print(f"✓ TBDATE: {tbdate_rpvb} → REPTDT: {reptdt}, PREVDT: {prevdt}")
    except Exception as e:
        print(f"✗ Error: {e}")
        today = date.today()
        reptdate = end_of_prev_month(today)
        prevdate = end_of_prev_month(reptdate)
        reptdt, prevdt = mmyy_format(reptdate), mmyy_format(prevdate)
        print(f"  Fallback: REPTDT={reptdt}, PREVDT={prevdt}")
    
    print("\n" + "=" * 60 + "\nProcessing SRSDATA dates\n" + "=" * 60)
    
    try:
        line = read_first_line(BASE_INPUT / "SRSDATA.txt")
        tbdate_srs = line[0:8]
        
        if tbdate_srs.isdigit() and len(tbdate_srs) == 8:
            srs_tb_date = yyyymmdd_to_date(tbdate_srs)
            srstdt = mmyy_format(srs_tb_date)
            print(f"✓ TBDATE: {tbdate_srs} → SRSTDT: {srstdt}")
        else:
            match = re.search(r'(\d{8})', tbdate_srs)
            if match:
                srs_tb_date = yyyymmdd_to_date(match.group(1))
                srstdt = mmyy_format(srs_tb_date)
                print(f"✓ Extracted date: {match.group(1)} → SRSTDT: {srstdt}")
            else:
                srstdt = reptdt
                print(f"⚠ Using REPTDT as fallback: {srstdt}")
    except Exception as e:
        print(f"✗ Error: {e}")
        srstdt = reptdt
        print(f"  Using REPTDT as fallback: {srstdt}")
    
    print("\n" + "=" * 60 + "\nDate validation\n" + "=" * 60)
    if reptdt != srstdt:
        error_msg = f"THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:{srstdt})"
        print(f"✗ {error_msg}")
        raise RuntimeError(error_msg)
    print(f"✓ REPTDT={reptdt} matches SRSTDT={srstdt}")
    
    print("\n" + "=" * 60 + "\nReading and filtering data\n" + "=" * 60)
    
    rpvb1 = read_rpvdata()
    print(f"✓ RPVB1: {len(rpvb1)} records")
    
    if len(rpvb1) > 0:
        rpvb2 = rpvb1.filter(pl.col("ACCTSTA").is_in(["D", "S", "R"]))
        rpvb3 = rpvb2.filter(pl.col("DATESTLD").is_not_null()) if 'DATESTLD' in rpvb2.columns else rpvb2.filter(pl.lit(False))
        print(f"✓ RPVB2: {len(rpvb2)} records (ACCTSTA in D,S,R)")
        print(f"✓ RPVB3: {len(rpvb3)} records (with DATESTLD)")
    else:
        rpvb2 = rpvb3 = rpvb1
    
    print("\n" + "=" * 60 + "\nCreating output datasets\n" + "=" * 60)
    
    repo_prev_path = BASE_OUTPUT / "REPO" / f"REPS_{prevdt}.parquet"
    repo_curr_path = BASE_OUTPUT / "REPO" / f"REPS_{reptdt}.parquet"
    repowh_path = BASE_OUTPUT / "REPOWH" / f"REPS_{reptdt}.parquet"
    
    try:
        repo_prev = pl.read_parquet(repo_prev_path)
        print(f"✓ Loaded previous: {len(repo_prev)} records")
        
        if len(rpvb3) > 0 and len(repo_prev) > 0:
            all_cols = list(set(rpvb3.columns) | set(repo_prev.columns))
            for col in all_cols:
                if col not in rpvb3.columns:
                    rpvb3 = rpvb3.with_columns(pl.lit(None).alias(col))
                if col not in repo_prev.columns:
                    repo_prev = repo_prev.with_columns(pl.lit(None).alias(col))
            rpvb3, repo_prev = rpvb3.select(all_cols), repo_prev.select(all_cols)
    except Exception:
        repo_prev = pl.DataFrame()
    
    repo_reps = rpvb3 if len(repo_prev) == 0 else pl.concat([rpvb3, repo_prev], how="vertical", rechunk=True)
    write_parquet(repo_reps, repo_curr_path)
    print(f"✓ REPO: {len(repo_reps)} records")
    
    repowh_reps = repo_reps.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first") if len(repo_reps) > 0 and 'MNIACTNO' in repo_reps.columns else repo_reps
    write_parquet(repowh_reps, repowh_path)
    print(f"✓ REPOWH: {len(repowh_reps)} records ({len(repo_reps)-len(repowh_reps)} duplicates removed)")
    
    print("\n" + "=" * 60 + "\nSUMMARY\n" + "=" * 60)
    print(f"TBDATE RPVBDATA: {tbdate_rpvb if 'tbdate_rpvb' in locals() else 'N/A'}")
    print(f"TBDATE SRSDATA: {tbdate_srs if 'tbdate_srs' in locals() else 'N/A'}")
    print(f"REPTDT: {reptdt} | PREVDT: {prevdt} | SRSTDT: {srstdt}")
    print(f"RPVB1: {len(rpvb1)} | RPVB2: {len(rpvb2)} | RPVB3: {len(rpvb3)}")
    print(f"REPO: {len(repo_reps)} | REPOWH: {len(repowh_reps)}")
    print("=" * 60 + "\n✓ Processing completed\n" + "=" * 60)

if __name__ == "__main__":
    main()
