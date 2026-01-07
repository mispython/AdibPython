from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path

base_input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input")
base_output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output")

RPVBDATA_TXT_PATH = base_input_path / "RPVBDATA.txt"
SRSDATA_TXT_PATH = base_input_path / "SRSDATA.txt"

REPO_DIR = base_output_path / "REPO"
REPOWH_DIR = base_output_path / "REPOWH"

def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def end_of_month(d: date) -> date:
    nxt = date(d.year + (d.month == 12), 1 if d.month == 12 else d.month + 1, 1)
    return nxt - timedelta(days=1)

def MMYYN4(d: date) -> str:
    return f"{d.month:02d}{d.year % 100:02d}"

def extract_rpvb_date():
    with open(RPVBDATA_TXT_PATH, 'r') as f:
        first_line = f.readline().strip()
    return first_line.split()[1] if first_line.startswith('0') else ""

def extract_srs_date():
    with open(SRSDATA_TXT_PATH, 'r') as f:
        first_line = f.readline().strip()
    return first_line[:8] if first_line else ""

def read_rpvdata_txt():
    records = []
    with open(RPVBDATA_TXT_PATH, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('0'):
                continue
                
            elif line.startswith('1'):
                parts = line.split()
                if len(parts) >= 15:
                    record = {
                        'MNIACTNO': parts[1] if len(parts) > 1 else '',
                        'BRANCHNO': parts[2] if len(parts) > 2 else '',
                        'NAME': ' '.join(parts[3:8]) if len(parts) > 8 else parts[3] if len(parts) > 3 else '',
                        'ACCTSTA': parts[8] if len(parts) > 8 else '',
                        'PRSTCOND': parts[9] if len(parts) > 9 else '',
                        'REGCARD': parts[10] if len(parts) > 10 else '',
                        'IGNTKEY': parts[11] if len(parts) > 11 else '',
                        'ACCTWOFF': parts[12] if len(parts) > 12 else '',
                        'MODEREPO': parts[13] if len(parts) > 13 else '',
                        'REPOSTAT': parts[14] if len(parts) > 14 else '',
                        'MODEDISP': parts[15] if len(parts) > 15 else '',
                        'YY1': parts[16][:4] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                        'MM1': parts[16][4:6] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                        'DD1': parts[16][6:8] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                    }
                    records.append(record)
    
    return pl.DataFrame(records)

def write_parquet(df: pl.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), p)

def main():
    TBDATE_STR_RPVB = extract_rpvb_date()
    TBDATE_STR_SRS = extract_srs_date()
    
    tb_date_rpvb = yyyymmdd_to_date(TBDATE_STR_RPVB)
    srs_tb_date = yyyymmdd_to_date(TBDATE_STR_SRS)
    
    REPTDATE = end_of_month(date(tb_date_rpvb.year, tb_date_rpvb.month, 1) - timedelta(days=1))
    PREVDATE = end_of_month(date(REPTDATE.year, REPTDATE.month, 1) - timedelta(days=1))
    
    REPTDT = MMYYN4(REPTDATE)
    PREVDT = MMYYN4(PREVDATE)
    SRSTDT = MMYYN4(srs_tb_date)
    
    if REPTDT != SRSTDT:
        raise RuntimeError(f"Date validation failed: REPTDT={REPTDT}, SRSTDT={SRSTDT}")
    
    raw_data_df = read_rpvdata_txt()
    
    if len(raw_data_df) > 0:
        RPVB1 = (
            raw_data_df
            .with_columns([
                pl.col("NAME").str.to_uppercase(),
                pl.col("ACCTSTA").str.to_uppercase(),
                pl.col("PRSTCOND").str.to_uppercase(),
                pl.col("REGCARD").str.to_uppercase(),
                pl.col("IGNTKEY").str.to_uppercase(),
                pl.col("ACCTWOFF").str.to_uppercase(),
                pl.col("MODEREPO").str.to_uppercase(),
                pl.col("REPOSTAT").str.to_uppercase(),
                pl.col("MODEDISP").str.to_uppercase(),
            ])
            .with_columns([
                pl.when(pl.any_horizontal([pl.col("MM1").is_null(), pl.col("DD1").is_null(), pl.col("YY1").is_null()]))
                  .then(pl.lit(None))
                  .otherwise(pl.datetime(pl.col("YY1"), pl.col("MM1"), pl.col("DD1")).cast(pl.Date))
                  .alias("DATEWOFF"),
            ])
            .drop(["YY1", "MM1", "DD1"])
        )
    else:
        RPVB1 = pl.DataFrame({
            'MNIACTNO': [], 'BRANCHNO': [], 'NAME': [], 'ACCTSTA': [], 
            'PRSTCOND': [], 'REGCARD': [], 'IGNTKEY': [], 'ACCTWOFF': [], 
            'MODEREPO': [], 'REPOSTAT': [], 'MODEDISP': [], 'DATEWOFF': []
        })
    
    if len(RPVB1) > 0:
        RPVB2 = RPVB1.filter(pl.col("ACCTSTA").is_in(["D","S","R"]))
        RPVB3 = RPVB2.filter(pl.col("DATEWOFF").is_not_null())
    else:
        RPVB2 = RPVB1
        RPVB3 = RPVB1
    
    REPO_PREV_PATH = REPO_DIR / f"REPS_{PREVDT}.parquet"
    REPO_CURR_PATH = REPO_DIR / f"REPS_{REPTDT}.parquet"
    REPOWH_PATH = REPOWH_DIR / f"REPS_{REPTDT}.parquet"
    
    rpbv3_schema = RPVB3.schema if len(RPVB3) > 0 else None
    
    try:
        REPO_PREV = pl.read_parquet(REPO_PREV_PATH)
        if rpbv3_schema and len(REPO_PREV) > 0:
            for col_name, col_type in rpbv3_schema.items():
                if col_name not in REPO_PREV.columns:
                    REPO_PREV = REPO_PREV.with_columns(pl.lit(None).cast(col_type).alias(col_name))
                else:
                    REPO_PREV = REPO_PREV.with_columns(pl.col(col_name).cast(col_type))
            if len(RPVB3.columns) > 0:
                REPO_PREV = REPO_PREV.select(RPVB3.columns)
    except Exception:
        if rpbv3_schema:
            REPO_PREV = pl.DataFrame(schema=rpbv3_schema)
        else:
            REPO_PREV = pl.DataFrame()
    
    if len(REPO_PREV) == 0:
        REPO_REPS = RPVB3
    else:
        REPO_REPS = pl.concat([RPVB3, REPO_PREV], how="vertical", rechunk=True)
    
    write_parquet(REPO_REPS, REPO_CURR_PATH)
    
    REPOWH_REPS = REPO_REPS.clone()
    if len(REPOWH_REPS) > 0 and 'MNIACTNO' in REPOWH_REPS.columns:
        REPOWH_REPS = REPOWH_REPS.sort("MNIACTNO").unique(subset=["MNIACTNO"], keep="first")
    write_parquet(REPOWH_REPS, REPOWH_PATH)
    
    print(f"Processing completed. Records: RPVB1={len(RPVB1)}, RPVB3={len(RPVB3)}, REPO={len(REPO_REPS)}, REPOWH={len(REPOWH_REPS)}")

if __name__ == "__main__":
    main()
